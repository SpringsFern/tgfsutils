import lmdb
import gc
from sys import argv
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bson import decode_file_iter, encode, decode
from pyrogram.file_id import FileId

if len(argv)<=3 or not argv[3].isdigit():
    print("Usage: python bson_util.py <old db name> <new db name> <bot_id>")
    exit(1)

DUMP_PATH = Path("mongodump", argv[2])
DUMP_PATH.mkdir(parents=True, exist_ok=True)

INPUT_BSON = Path("oldbson", argv[1])
FILES_BSON = Path(DUMP_PATH, "files.bson")
MAPLINKS_BSON = Path(DUMP_PATH, "maplinks.bson")
USERS_BSON = Path(DUMP_PATH, "users.bson")

LMDB_PATH = "./files.lmdb"
LMDB_MAP_SIZE = 1024 * 1024 * 1024 * 200

WRITE_BATCH = 10_000

BOT_ID = int(argv[3])


def normalize_time(t):
    if isinstance(t, datetime):
        return t
    if isinstance(t, str):
        return datetime.fromisoformat(t)
    return datetime.fromtimestamp(float(t), tz=timezone.utc)

def deep_set(target: Dict[str, Any], update: Dict[str, Any]) -> None:
    for k, v in update.items():
        if isinstance(v, dict) and isinstance(target.get(k), dict):
            deep_set(target[k], v)
        else:
            target[k] = v


def build_files_update(old: dict) -> tuple[int, dict]:
    decoded = FileId.decode(old["file_id"])
    media_id = decoded.media_id
    user_id = int(old["user_id"])

    update = {
        "dc_id": decoded.dc_id,
        "file_name": old["file_name"],
        "is_deleted": old.get("restricted", False),
        "mime_type": old["mime_type"],
        "size": int(old["file_size"]),
        "thumb_size": decoded.thumbnail_size,
        "users": {
            str(user_id): {
                "chat_id": user_id,
                "message_id": old.get("msg_id") or 0,
                "added_at": normalize_time(old["time"]),
            }
        },
        "location": {
            str(BOT_ID): {
                "access_hash": decoded.access_hash,
                "file_reference": decoded.file_reference,
            }
        },
    }

    file_ids = old.get("file_ids")
    if isinstance(file_ids, dict):
        for bot_id, fid in file_ids.items():
            try:
                d = FileId.decode(fid)
                update["location"][str(bot_id)] = {
                    "access_hash": d.access_hash,
                    "file_reference": d.file_reference,
                }
            except Exception:
                continue

    return media_id, update

def migrate_files_lmdb():
    env = lmdb.open(
        LMDB_PATH,
        map_size=LMDB_MAP_SIZE,
        subdir=True,
        max_dbs=1,
        lock=True,
        sync=False,
        metasync=False,
        readahead=True,
    )

    gc.disable()

    txn = env.begin(write=True)
    count = 0

    with open(Path(INPUT_BSON, "file.bson"), "rb") as f:
        for old in decode_file_iter(f):

            media_id, update = build_files_update(old)
            key = str(media_id).encode()

            existing = txn.get(key)
            if existing:
                doc = decode(existing)
            else:
                doc = {"_id": media_id}

            deep_set(doc, update)
            txn.put(key, encode(doc))

            count += 1
            if count % WRITE_BATCH == 0:
                txn.commit()
                txn = env.begin(write=True)
                print(f"[files] processed {count}")

    txn.commit()
    gc.enable()
    env.sync()
    env.close()


def write_files_bson():
    env = lmdb.open(LMDB_PATH, readonly=True, lock=False)

    with env.begin() as txn, open(FILES_BSON, "wb") as out:
        cursor = txn.cursor()
        for _, value in cursor:
            out.write(value)

    env.close()


def write_maplinks_bson():
    with open(Path(INPUT_BSON, "file.bson"), "rb") as f, open(MAPLINKS_BSON, "wb") as out:
        for old in decode_file_iter(f):
            decoded = FileId.decode(old["file_id"])
            out.write(encode({
                "_id": old["_id"],
                "media_id": decoded.media_id,
                "user_id": int(old["user_id"]),
            }))

def write_users_bson():
    user_ids = {}
    with open(Path(INPUT_BSON, "blacklist.bson"), "rb") as f:
        for doc in decode_file_iter(f):
            user_ids[doc["id"]] = doc["ban_date"]

    with open(Path(INPUT_BSON, "users.bson"), "rb") as f, open(USERS_BSON, "wb") as coll:
        for doc in decode_file_iter(f):
            encoded_doc = encode({
                "_id": doc["id"],
                "ban_date": user_ids.get(doc["id"], None),
                "curt_op": 0,
                "join_date": normalize_time(doc["join_date"]),
                "op_id": 0,
                "preferred_lang": "en",
                "warns": 0
            })
            coll.write(encoded_doc)

if __name__ == "__main__":
    print("▶reducing files into LMDB")
    migrate_files_lmdb()

    print("▶ writing files.bson")
    write_files_bson()

    print("▶ writing maplinks.bson")
    write_maplinks_bson()

    print("▶ writing users.bson")
    write_users_bson()
