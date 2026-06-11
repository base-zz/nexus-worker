from __future__ import annotations

import os
import sys
import json
import redis
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

# Strict configuration: No defaults.
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
OUTBOUND_QUEUE = os.environ['OUTBOUND_SYNC_QUEUE']
PROCESSING_QUEUE = f"{OUTBOUND_QUEUE}:processing"
DLQ = f"{OUTBOUND_QUEUE}:dlq"
VPS_IP = os.environ['VPS_IP']
VPS_USER = os.environ['VPS_USER']
VPS_DB_PATH = os.environ['VPS_DB_PATH']
SSH_TIMEOUT = int(os.environ['SSH_TIMEOUT'])
MAX_BATCH_SIZE = int(os.environ['SYNC_MAX_BATCH'])
VALID_TABLES = {"pricing_logs", "fuel_logs", "marinas", "sync_events", "extractions"}
APPEND_ONLY_TABLES = {"pricing_logs", "fuel_logs", "sync_events", "extractions"}

# Redis connection pool
pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
r = redis.Redis(connection_pool=pool)

def _build_marina_upsert(data_dict: dict) -> str:
    """Idempotent UPDATE-then-INSERT for marinas table.

    Only overwrites existing VPS data if the incoming payload has a
    newer updated_at_utc. Falls back to INSERT OR IGNORE for new records.
    """
    marina_uid = data_dict.get("marina_uid")
    if not marina_uid:
        raise ValueError("marinas sync payload missing marina_uid")

    uid_escaped = str(marina_uid).replace("'", "''")
    incoming_ts = str(data_dict.get("updated_at_utc", "")).replace("'", "''")

    # Build SET clause for UPDATE and cols/vals for INSERT
    set_parts: list[str] = []
    insert_cols = ["marina_uid"]
    insert_vals = [f"'{uid_escaped}'"]

    for col, val in data_dict.items():
        if col == "marina_uid":
            continue
        if val is None:
            set_parts.append(f"{col} = NULL")
            insert_vals.append("NULL")
        elif isinstance(val, (int, float)):
            set_parts.append(f"{col} = {val}")
            insert_vals.append(str(val))
        else:
            escaped = str(val).replace("'", "''")
            set_parts.append(f"{col} = '{escaped}'")
            insert_vals.append(f"'{escaped}'")
        insert_cols.append(col)

    set_clause = ", ".join(set_parts)
    ts_guard = (
        f"AND (updated_at_utc IS NULL OR updated_at_utc < '{incoming_ts}')"
        if incoming_ts
        else ""
    )

    return (
        f"BEGIN IMMEDIATE; "
        f"UPDATE marinas SET {set_clause} "
        f"WHERE marina_uid = '{uid_escaped}' {ts_guard}; "
        f"INSERT OR IGNORE INTO marinas ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(insert_vals)}); "
        f"COMMIT;"
    )


def _build_sync_event_sql(
    data_dict: dict,
    target_table: str,
    fetch_method: str,
) -> str:
    """Build an append-only sync_events audit record for a successful sync."""
    marina_uid = str(data_dict.get("marina_uid", "")).replace("'", "''")
    after_hash = str(data_dict.get("extraction_hash", "")).replace("'", "''")
    method = str(fetch_method).replace("'", "''")

    entity_type = {
        "pricing_logs": "pricing_log",
        "fuel_logs": "fuel_log",
        "marinas": "marina",
        "extractions": "extraction",
        "sync_events": "sync_event",
    }.get(target_table, "unknown")

    return (
        f"INSERT INTO sync_events ("
        f"marina_uid, entity_type, entity_ref, event_type, reason_tag, "
        f"after_hash, sync_dirty_before, sync_dirty_after, "
        f"master_acknowledged, occurred_at_utc, fetch_method"
        f") VALUES ("
        f"'{marina_uid}', '{entity_type}', '{marina_uid}', 'data_synced', "
        f"'sync_worker_success', '{after_hash}', 1, 0, 0, "
        f"datetime('now'), '{method}'"
        f");"
    )


def build_secure_sql(table_name, data_dict):
    if table_name not in VALID_TABLES:
        raise ValueError(f"Security Alert: Unauthorized table: {table_name}")

    # Marinas: conditional upsert to prevent stale overwrites
    if table_name == "marinas":
        return _build_marina_upsert(data_dict)

    # Append-only tables: INSERT with optional extraction_hash dedup
    cols, vals = [], []
    for col, val in data_dict.items():
        cols.append(col)
        if val is None:
            vals.append("NULL")
        elif isinstance(val, (int, float)):
            vals.append(str(val))
        else:
            vals.append(f"'{str(val).replace(chr(39), chr(39)+chr(39))}'")

    if table_name in APPEND_ONLY_TABLES and "extraction_hash" in data_dict:
        hash_val = str(data_dict["extraction_hash"]).replace("'", "''")
        marina_uid = str(data_dict.get("marina_uid", "")).replace("'", "''")
        return (
            f"BEGIN IMMEDIATE; "
            f"INSERT INTO {table_name} ({', '.join(cols)}) "
            f"SELECT {', '.join(vals)} "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {table_name} "
            f"  WHERE marina_uid = '{marina_uid}' AND extraction_hash = '{hash_val}'"
            f"); "
            f"COMMIT;"
        )

    return (
        f"BEGIN IMMEDIATE; INSERT INTO {table_name} ({', '.join(cols)}) "
        f"VALUES ({', '.join(vals)}); COMMIT;"
    )

def execute_remote_batch(sql_statements):
    """Executes SQL statements via stdin pipe."""
    full_sql = "\n".join(sql_statements)
    cmd = ["ssh", "-o", "BatchMode=yes", f"{VPS_USER}@{VPS_IP}", "sqlite3", "-cmd", ".timeout 5000", VPS_DB_PATH]
    
    try:
        result = subprocess.run(cmd, input=full_sql, capture_output=True, text=True, timeout=SSH_TIMEOUT)
        if result.returncode != 0:
            print(f"[X] Remote SQL Error: {result.stderr}", file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"[!] SSH Transport Error: {e}", file=sys.stderr)
        return False

def requeue_processing_items():
    """Moves items from processing back to the start of the outbound queue."""
    while True:
        # Move RIGHT of processing to LEFT of outbound (prioritizes retries)
        item = r.rpoplpush(PROCESSING_QUEUE, OUTBOUND_QUEUE)
        if not item: break

def main():
    # Ensure any previous crash/restart jobs are moved to the front of the line
    requeue_processing_items()
    print(f"[*] Robust Sync Worker active. Monitoring '{OUTBOUND_QUEUE}'...")
    
    while True:
        try:
            # Atomic Move: Pull from outbound to processing
            item = r.blmove(OUTBOUND_QUEUE, PROCESSING_QUEUE, 5, 'RIGHT', 'LEFT')
            if not item: continue
            
            batch = [item]
            while len(batch) < MAX_BATCH_SIZE:
                next_item = r.rpoplpush(OUTBOUND_QUEUE, PROCESSING_QUEUE)
                if not next_item: break
                batch.append(next_item)
            
            sql_statements, valid_items = [], []
            for raw in batch:
                try:
                    p = json.loads(raw)
                    if not isinstance(p, dict):
                        raise TypeError("Payload not a dict")
                    target_table = p["target_table"]
                    data = p["data"]
                    fetch_method = p.get("fetch_method", "")

                    # Target table SQL
                    sql_statements.append(build_secure_sql(target_table, data))
                    # Audit trail SQL
                    sql_statements.append(
                        _build_sync_event_sql(data, target_table, fetch_method)
                    )
                    valid_items.append(raw)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                    print(f"[!] Invalid payload format: {e}")
                    r.lpush(DLQ, raw)
                    r.lrem(PROCESSING_QUEUE, 1, raw)

            if execute_remote_batch(sql_statements):
                for item in valid_items:
                    r.lrem(PROCESSING_QUEUE, 1, item)
                print(f"[✔] Successfully synced {len(valid_items)} items.")
            else:
                print("[!] Sync failed. Items remain in processing for retry.")
                time.sleep(5)

        except redis.ConnectionError:
            print("[X] Redis offline. Retrying...")
            time.sleep(5)
        except redis.TimeoutError:
            print("[X] Redis timeout. Retrying...")
            time.sleep(5)
        except Exception as e:
            print(f"[X] Worker Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()