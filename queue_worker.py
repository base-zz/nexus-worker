import os
import json
import time
import redis
import ollama
import sqlite3
from dotenv import load_dotenv

# Load variables from /opt/nexus-worker/.env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Configuration pulled strictly from environment. Will crash immediately if key is missing.
REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
QUEUE_NAME = os.environ['INBOUND_QUEUE']
SYNC_QUEUE_NAME = os.environ['OUTBOUND_SYNC_QUEUE']
MODEL_NAME = os.environ['OLLAMA_MODEL']
LOCAL_DB_PATH = os.environ['LOCAL_DB_PATH']

# Global reference for Redis connection, initialized in main()
r = None

def init_local_db():
    """Ensures a local backup database exists on srv01"""
    conn = sqlite3.connect(LOCAL_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS extractions (
            id INTEGER PRIMARY KEY,
            url TEXT,
            extracted_insight TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def save_local_backup(job_id, url, insight):
    """Saves a copy of the AI response locally on srv01"""
    try:
        conn = sqlite3.connect(LOCAL_DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO extractions (id, url, extracted_insight)
            VALUES (?, ?, ?)
        ''', (job_id, url, insight))
        conn.commit()
        conn.close()
        print(f"[✔] Local backup database updated for Job ID {job_id}")
    except Exception as e:
        print(f"[X] Failed to write local backup: {e}")

def process_job(job_data):
    try:
        payload = json.loads(job_data)
        job_id = payload.get('id')
        url = payload.get('url')
        
        print(f"\n[+] Processing Job ID {job_id} for URL: {url}")
        
        system_prompt = "You are a structured data extraction assistant."
        user_content = f"Analyze this scraped data payload and extract key details: {payload}"
        
        # Run inference on the GPU
        response = ollama.chat(model=MODEL_NAME, messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_content}
        ])
        
        ai_insight = response['message']['content']
        
        # --- THE DUAL-QUEUE WORKFLOW ---
        # 1. Write the backup locally to srv01
        save_local_backup(job_id, url, ai_insight)
        
        # 2. Package data and drop it onto the outbound sync queue for retry resilience
        sync_payload = {
            "id": job_id,
            "insight": ai_insight
        }
        r.lpush(SYNC_QUEUE_NAME, json.dumps(sync_payload))
        print(f"[->] Job ID {job_id} forwarded to local outbound sync queue.")
        
    except Exception as e:
        print(f"[X] Error processing job: {e}")

def main():
    global r
    
    # Initialize local SQLite DB configuration on startup
    init_local_db()
    
    # Configure connection pool with health checks to keep the socket alive indefinitely
    pool = redis.ConnectionPool(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        decode_responses=True,
        socket_timeout=None,          
        socket_connect_timeout=None,
        health_check_interval=30      
    )
    r = redis.Redis(connection_pool=pool)
    
    print(f"[*] Worker listening on Redis queue '{QUEUE_NAME}'...")
    print(f"[*] Targeting local Ollama model: '{MODEL_NAME}'")

    while True:
        try:
            result = r.blpop(QUEUE_NAME, timeout=0)
            if result:
                _, job_data = result
                process_job(job_data)
                
        except redis.exceptions.TimeoutError:
            print("[*] Connection idled out. Re-establishing heartbeat link...")
            time.sleep(1)
            continue
        except redis.exceptions.ConnectionError:
            print("[X] Connection lost. Retrying in 5 seconds...")
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            print("\n[-] Worker shutting down gracefully.")
            break

if __name__ == "__main__":
    main()
