import asyncio
import traceback
from os import getenv
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pyrogram.file_id import FileId
from tqdm import tqdm

progress_lock = asyncio.Lock()

class MongoTGFS():
    client: AsyncIOMotorClient
    db: AsyncIOMotorDatabase
    files: AsyncIOMotorCollection
    groups: AsyncIOMotorCollection
    users: AsyncIOMotorCollection
    config: AsyncIOMotorCollection

    async def connect(self, uri: str, dbname) -> None:
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[dbname]

        self.files = self.db.files
        self.groups = self.db.groups
        self.users = self.db.users
        self.config = self.db.app_config

    async def close(self) -> None:
        self.client.close()

    async def init_db(self) -> None:
        await self._create_indexes()

    async def _create_indexes(self) -> None:
        await self.files.create_index("users.user_id")
        await self.files.create_index("is_deleted")
        await self.files.create_index("users.added_at")

        await self.groups.create_index("user_id")
        await self.groups.create_index([("user_id", 1), ("created_at", -1)])

        await self.users.create_index("ban_date")
        await self.users.create_index("warns")


def normalize_time(t):
    if isinstance(t, datetime):
        return t
    if isinstance(t, str):
        return datetime.fromisoformat(t)
    return datetime.fromtimestamp(float(t), tz=timezone.utc)


BOT_ID = None

async def migrate_old_file(tgfs: MongoTGFS, old: dict) -> None:
    decoded = FileId.decode(old["file_id"])
    media_id = decoded.media_id

    user_id = int(old["user_id"])

    location_update = {
        f"location.{BOT_ID}": {
            "access_hash": decoded.access_hash,
            "file_reference": decoded.file_reference
        }
    }

    file_ids = old.get("file_ids")
    if isinstance(file_ids, dict):
        for bot_id, fid in file_ids.items():
            try:
                d = FileId.decode(fid)
                location_update[f"location.{int(bot_id)}"] = {
                    "access_hash": d.access_hash,
                    "file_reference": d.file_reference,
                }
            except Exception:
                continue
    update = {
        "dc_id": decoded.dc_id,
        "file_name": old["file_name"],
        "is_deleted": old.get("restricted", False),
        "mime_type": old["mime_type"],
        "size": int(old["file_size"]),
        "thumb_size": decoded.thumbnail_size,
        f"users.{user_id}": {
            "chat_id": user_id,
            "message_id": old.get("msg_id") or 0,
            "added_at": normalize_time(old["time"]),
        },
        **location_update,
    }

    await tgfs.files.update_one(
        {"_id": media_id},
        {"$set": update},
        upsert=True,
    )


async def migrate_old_users(tgfs: MongoTGFS, old: dict) -> None:
    await tgfs.users.update_one(
        {"_id": old["id"]},
        {"$set": {
            "_id": old["id"],
            "ban_date": None,
            "curt_op": 0,
            "join_date": normalize_time(old["join_date"]),
            "op_id": 0,
            "preferred_lang": "en",
            "warns": 0
        }
        },
        upsert=True,
    )


async def migrate_blacklist(tgfs: MongoTGFS, old: dict) -> None:
    await tgfs.users.update_one(
        {"_id": old["id"]},
        {
            "$set": {
                "ban_date": normalize_time(old["ban_date"])
            }
        },
        upsert=False
    )

async def worker(name, queue, migrate_fn, tgfs, pbar, batch=20):
    local_count = 0
    while True:
        item = await queue.get()
        if item is None:
            if local_count:
                pbar.update(local_count)
            queue.task_done()
            break
        try:
            await migrate_fn(tgfs, item)
            local_count += 1
        except Exception as e:
            print(f"[{name}] Error:", e)
            traceback.print_exc()
        finally:
            if local_count >= batch:
                pbar.update(local_count)
                local_count = 0
            queue.task_done()

async def migrate_collection(
    collection,
    migrate_fn,
    tgfs,
    workers=20,
    queue_size=500,
    total_count = None,
    name=None,
    last_id=None
):
    queue = asyncio.Queue(maxsize=queue_size)

    with tqdm(total=total_count, desc=name) as pbar:
        worker_tasks = [
            asyncio.create_task(
                worker(f"W{i}", queue, migrate_fn, tgfs, pbar)
            )
            for i in range(workers)
        ]

        async for doc in batch_fetch(collection, batch_size=queue_size, last_id=last_id):
            await queue.put(doc)

        for _ in worker_tasks:
            await queue.put(None)

        await queue.join()
        await asyncio.gather(*worker_tasks)

async def batch_fetch(collection, batch_size=500, last_id=None):
    try:
        while True:
            query = {}
            if last_id:
                query["_id"] = {"$gt": last_id}

            docs = await collection.find(query)\
                .sort("_id", 1)\
                .limit(batch_size)\
                .to_list(length=batch_size)

            if not docs:
                break

            for doc in docs:
                yield doc

            last_id = docs[-1]["_id"]
    finally:
        print("Finished fetching from collection.")
        print("last_id:", last_id)

async def main():
    bot_id=getenv("BOT_ID", input("Enter Your Main Bot ID: "))
    if not bot_id.isdigit():
        print("Invalid Bot ID")
        return
    global BOT_ID
    BOT_ID = int(bot_id)
    old_uri = getenv("OLD_URI", input(f"Enter Old Mongo URI: "))
    old_dbname = getenv("OLD_DBNAME", input("Enter Old Database Name(Default: F2LxBot): ")) or "F2LxBot"
    new_uri = getenv("NEW_URI", input(f"Enter New Mongo URI: "))
    new_dbname = getenv("NEW_DBNAME", input("Enter New Database Name(Default: TGFS): ")) or "TGFS"
    if old_dbname == new_dbname and old_uri == new_uri:
        print("Old and New MongoDB configurations cannot be the same.")
        return
    tgfs = MongoTGFS()
    await tgfs.connect(new_uri, new_dbname)
    await tgfs.init_db()
    mongo_client = AsyncIOMotorClient(old_uri)
    old_db = mongo_client[old_dbname]

    workers = getenv("WORKERS", input("Enter number of worker tasks for migration (Default: 20): "))
    workers = int(workers) if workers.isdigit() else 20
    queue_size = getenv("QUEUE_SIZE", input("Enter no of documents to pre fetch from DB (Default: 500): "))
    queue_size = int(queue_size) if queue_size.isdigit() else 500

    print("Migrating files...")
    await migrate_collection(
        old_db.file,
        migrate_old_file,
        tgfs,
        workers=workers,
        queue_size=queue_size,
        total_count=await old_db.file.count_documents({}),
        name="Files",
        last_id=None
    )

    print("Migrating users...")
    await migrate_collection(
        old_db.users,
        migrate_old_users,
        tgfs,
        workers=workers,
        queue_size=queue_size,
        total_count=await old_db.users.count_documents({}),
        name="Users",
        last_id=None
    )

    print("Migrating blacklist...")
    await migrate_collection(
        old_db.blacklist,
        migrate_blacklist,
        tgfs,
        workers=workers,
        queue_size=queue_size,
        total_count=await old_db.blacklist.count_documents({}),
        name="Blacklist",
        last_id=None
    )

    print("Setting version...")
    await tgfs.config.update_one(
        {"_id": "VERSION"},
        {"$set": {"value": "0.0.1"}},
        upsert=True,
    )
    mongo_client.close()
asyncio.run(main())
