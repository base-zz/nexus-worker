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

# Redis connection pool
pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
r = redis.Redis(connection_pool=pool)

def build_secure_sql(table_name, data_dict):
    if table_name not in VALID_TABLES:
        raise ValueError(f"Security Alert: Unauthorized table: {table_name}")
    
    cols, vals = [], []
    for col, val in data_dict.items():
        cols.append(col)
        if val is None: vals.append("NULL")
        elif isinstance(val, (int, float)): vals.append(str(val))
        else: vals.append(f"'{str(val).replace("'", "''")}'")
            
    return f"BEGIN IMMEDIATE; INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join(vals)}); COMMIT;"

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
            item = r.blmove(OUTBOUND_QUEUE, PROCESSING_QUEUE, 'RIGHT', 'LEFT', timeout=5)
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
                    if not isinstance(p, dict): raise TypeError("Payload not a dict")
                    sql_statements.append(build_secure_sql(p['target_table'], p['data']))
                    valid_items.append(raw)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                    print(f"[!] Invalid payload format: {e}")
                    r.lpush(DLQ, raw)
                    r.lrem(PROCESSING_QUEUE, 0, raw)
            
            if execute_remote_batch(sql_statements):
                for item in valid_items:
                    r.lrem(PROCESSING_QUEUE, 0, item)
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