import os
import time
import json
import redis
import paramiko
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = int(os.environ['REDIS_PORT'])
SYNC_QUEUE_NAME = os.environ['OUTBOUND_SYNC_QUEUE']
VPS_IP = os.environ['VPS_IP']
VPS_USER = os.environ['VPS_USER']
VPS_DB_PATH = os.environ['VPS_DB_PATH']

def open_vps_connection():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=VPS_IP, username=VPS_USER, timeout=10)
        return ssh
    except Exception as e:
        print(f"[X] Failed to open SSH connection to VPS: {e}")
        return None

def build_dynamic_insert(table_name, data_dict):
    """
    Dynamically constructs a pristine, native SQLite INSERT statement
    safely handling numbers, text, strings with quotes, and NULL values.
    """
    columns = []
    values = []
    
    for col, val in data_dict.items():
        columns.append(col)
        if val is None:
            values.append("NULL")
        elif isinstance(val, (int, float)):
            values.append(str(val))
        else:
            # Escape single quotes for SQL safety
            escaped_val = str(val).replace("'", "''")
            values.append(f"'{escaped_val}'")
            
    col_str = ", ".join(columns)
    val_str = ", ".join(values)
    
    # Use INSERT INTO or REPLACE INTO based on what makes sense for your logs
    return f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str});"

def execute_remote_write(ssh, table_name, data_dict):
    try:
        sql_statement = build_dynamic_insert(table_name, data_dict)
        
        # Build command utilizing your 5-second busy timeout for better-sqlite3 safety
        cmd = f"sqlite3 -cmd \".timeout 5000\" {VPS_DB_PATH} \"{sql_statement}\""
        
        _, stdout, stderr = ssh.exec_command(cmd, timeout=10)
        error = stderr.read().decode()
        
        if error:
            print(f"[X] VPS SQLite Error writing to {table_name}: {error}")
            return False
            
        print(f"[✔] Remote VPS Table '{table_name}' updated successfully.")
        return True
    except Exception as e:
        print(f"[!] Dynamic SSH write failed: {e}")
        return False

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print(f"[*] Multi-Table Sync Worker active. Monitoring '{SYNC_QUEUE_NAME}'...")

    while True:
        try:
            # 1. DEEP SLEEP: No sockets open, CPU core can go completely dormant.
            result = r.blpop(SYNC_QUEUE_NAME, timeout=0)
            
            if result:
                # 2. WAKE UP
                _, first_job_data = result
                print("\n[+] Sync burst triggered. Waking up...")
                
                ssh = open_vps_connection()
                if not ssh:
                    r.rpush(SYNC_QUEUE_NAME, first_job_data)
                    time.sleep(30)
                    continue
                
                # Execute the first item
                payload = json.loads(first_job_data)
                success = execute_remote_write(ssh, payload['target_table'], payload['data'])
                if not success:
                    r.rpush(SYNC_QUEUE_NAME, first_job_data)
                
                # 3. THE BATCH FLUSH: Drain any other pending updates over the same single pipe
                while True:
                    next_job_data = r.rpop(SYNC_QUEUE_NAME)
                    if not next_job_data:
                        break
                        
                    payload = json.loads(next_job_data)
                    success = execute_remote_write(ssh, payload['target_table'], payload['data'])
                    if not success:
                        r.rpush(SYNC_QUEUE_NAME, next_job_data)
                        break
                
                # 4. VANISH: Sever connection and go back to sleep
                ssh.close()
                print("[-] Queue drained. Connection severed. Back to zero-overhead sleep.")
                
        except redis.exceptions.RedisError as re:
            print(f"[X] Redis Error: {re}")
            time.sleep(5)
        except Exception as e:
            print(f"[X] Runtime Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
