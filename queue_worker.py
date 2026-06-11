from __future__ import annotations

import os
import json
import time
import redis
import ollama
import sqlite3
from dotenv import load_dotenv

# Strict loading
load_dotenv()
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
QUEUE_NAME = os.environ['INBOUND_QUEUE']
PROCESSING_QUEUE = f"{QUEUE_NAME}:processing"
FAILED_QUEUE = f"{QUEUE_NAME}:failed"
SYNC_QUEUE_NAME = os.environ['OUTBOUND_SYNC_QUEUE']
MODEL_NAME = os.environ['OLLAMA_MODEL']
LOCAL_DB_PATH = os.environ['LOCAL_DB_PATH']

def init_local_db():
    conn = sqlite3.connect(LOCAL_DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extractions (
                id TEXT PRIMARY KEY,
                url TEXT,
                extracted_insight TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    finally:
        conn.close()

def process_job(r, job_data):
    """Encapsulated job processing logic."""
    try:
        payload = json.loads(job_data)
        job_id = str(payload.get('id'))
        url = payload.get('url')
        
        # Inference
        response = ollama.chat(model=MODEL_NAME, messages=[
            {'role': 'system', 'content': 'You are a structured data extraction assistant.'},
            {'role': 'user', 'content': f"Extract key details: {payload}"}
        ])
        ai_insight = response['message']['content']
        
        # Save Local
        conn = sqlite3.connect(LOCAL_DB_PATH)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO extractions (id, url, extracted_insight) VALUES (?, ?, ?)", 
                (job_id, url, ai_insight)
            )
            conn.commit()
        finally:
            conn.close()
        
        # Forward to Sync Queue
        sync_payload = {
            "target_table": "extractions",
            "data": {"id": job_id, "insight": ai_insight}
        }
        r.lpush(SYNC_QUEUE_NAME, json.dumps(sync_payload, default=str))
        
        # Cleanup atomic processing queue
        r.lrem(PROCESSING_QUEUE, 0, job_data)
        print(f"[✔] Job {job_id} successfully processed and queued for sync.")
        
    except Exception as e:
        print(f"[X] Permanent failure on Job ID: {e}")
        # Move failed job to failed queue and remove from processing
        r.lpush(FAILED_QUEUE, job_data)
        r.lrem(PROCESSING_QUEUE, 0, job_data)

def main():
    init_local_db()
    pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    
    while True:
        try:
            # Atomic Move: Pull job to processing queue
            job_data = r.blmove(QUEUE_NAME, PROCESSING_QUEUE, 'RIGHT', 'LEFT', timeout=5)
            if job_data:
                process_job(r, job_data)
        except Exception as e:
            print(f"[!] Critical loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()