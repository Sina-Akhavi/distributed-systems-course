import socket
import threading
import json
import sqlite3
import traceback
import time
import uuid
import argparse 
from datetime import datetime, timezone
import os


def load_env(filepath='.env'):
    """Loads variables from a .env file into os.environ."""
    try:
        with open(filepath) as f:
            for line in f:
                # Ignore empty lines and comments
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    except FileNotFoundError:
        print(f"Warning: {filepath} not found. Using defaults.")

# Now it is safe to call the function
load_env()

# Read from environment variables, falling back to defaults if missing
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', 65432))
DB_FILE = os.environ.get('DB_FILE', 'bank.db')


def init_db():
    """Initializes the database and creates the accounts table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # The CHECK constraint enforces Consistency at the database level
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            balance INTEGER CHECK(balance >= 0)
        )
    ''')
    # Insert some dummy data for testing
    cursor.execute('INSERT OR IGNORE INTO accounts (id, balance) VALUES ("A1001", 1000)')
    cursor.execute('INSERT OR IGNORE INTO accounts (id, balance) VALUES ("A1002", 500)')
    conn.commit()
    conn.close()

def process_transaction(req):
    """Processes a single JSON request and returns a JSON response."""

    # Artificially slow down the transaction to test the connection limit
    time.sleep(2)

    # Validate request structure
    if not isinstance(req, dict):
        return {"status": "error", "message": "Malformed request"}
    
    action = req.get("action")
    account = req.get("account")
    
    if not action or not account:
        return {"status": "error", "message": "Missing fields"}

    # We open a new connection per thread to ensure thread safety
    # timeout=10 allows SQLite to queue concurrent writes rather than failing immediately
    conn = sqlite3.connect(DB_FILE, timeout=10.0)
    cursor = conn.cursor()
    
    try:
        # BEGIN IMMEDIATE locks the database for writing immediately,
        # preventing dirty reads and deadlocks (Isolation)
        cursor.execute('BEGIN IMMEDIATE')

        # Check if account exists
        cursor.execute('SELECT balance FROM accounts WHERE id = ?', (account,))
        row = cursor.fetchone()
        
        if row is None:
            conn.rollback()
            return {"status": "error", "message": "Unknown account"}
            
        current_balance = row[0]

        if action == "balance":
            conn.commit()
            return {"status": "success", "account": account, "balance": current_balance}

        # Validate amount for deposit/withdraw
        amount = req.get("amount")
        if not isinstance(amount, (int, float)) or amount <= 0:
            conn.rollback()
            return {"status": "error", "message": "Negative or invalid amount"}

        if action == "deposit":
            new_balance = current_balance + amount
            cursor.execute('UPDATE accounts SET balance = ? WHERE id = ?', (new_balance, account))
            conn.commit() # Durability & Atomicity
            return {"status": "success", "account": account, "balance": new_balance}

        elif action == "withdraw":
            if current_balance < amount:
                conn.rollback()
                return {"status": "error", "message": "Insufficient funds"}
            
            new_balance = current_balance - amount
            cursor.execute('UPDATE accounts SET balance = ? WHERE id = ?', (new_balance, account))
            conn.commit()
            return {"status": "success", "account": account, "balance": new_balance}

        else:
            conn.rollback()
            return {"status": "error", "message": "Unsupported action"}

    except sqlite3.Error as e:
        # If any database constraint fails or an error occurs, undo everything
        conn.rollback()
        return {"status": "error", "message": f"Database error: {str(e)}"}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": "Internal server error"}
    finally:
        conn.close()

def log_request(req_id, ip, action, account, result, balance, latency):
    """Formats and writes the log entry to both console and file."""
    timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    
    # Construct the log line matching the assignment's exact example
    log_line = (f"{timestamp} req={req_id} ip={ip} action={action} "
                f"account={account} result={result} ")
    if balance is not None:
        log_line += f"balance={balance} "
    log_line += f"latency={latency}ms\n"
    
    print(log_line.strip())
    with open("server.log", "a") as f:
        f.write(log_line)


def handle_client(conn, addr, semaphore):
    """Handles communication with a single connected client and logs it."""
    client_ip = addr[0]
    req_id = uuid.uuid4().hex[:4] # Generate a short unique ID like '7f3a'
    start_time = time.time()
    
    action = "unknown"
    account = "unknown"
    result_status = "error"
    balance = None

    try:
        data = conn.recv(1024)
        if data:
            try:
                request = json.loads(data.decode('utf-8'))
                action = request.get("action", "unknown")
                account = request.get("account", "unknown")
                
                # Process the transaction
                response = process_transaction(request)
                
                result_status = response.get("status", "error")
                balance = response.get("balance")
                
            except json.JSONDecodeError:
                response = {"status": "error", "message": "Broken JSON"}
            
            # Send response back
            conn.sendall(json.dumps(response).encode('utf-8'))
    except Exception as e:
        traceback.print_exc()
        response = {"status": "error", "message": "Connection error"}
    finally:
        # Calculate latency in milliseconds
        latency = int((time.time() - start_time) * 1000)
        log_request(req_id, client_ip, action, account, result_status, balance, latency)
        conn.close()
        # Release the semaphore so another waiting client can enter
        semaphore.release()

def start_server():
    # Parse the maximum clients from the command line
    parser = argparse.ArgumentParser(description='Banking Server')
    parser.add_argument('--max-clients', type=int, default=10, 
                        help='Maximum number of simultaneous clients')
    args = parser.parse_args()

    # Create the semaphore to control concurrency
    client_semaphore = threading.Semaphore(args.max_clients)

    init_db()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow immediate port reuse if server restarts
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    
    print(f"[STARTING] Server is listening on {HOST}:{PORT} (Max Clients: {args.max_clients})")
    
    try:
        while True:
            conn, addr = server.accept()
            
            # Acquire the semaphore before spawning the thread.
            # If the limit is reached, the server pauses here until a client disconnects.
            client_semaphore.acquire()
            
            # Pass the semaphore into the handle_client function
            thread = threading.Thread(target=handle_client, args=(conn, addr, client_semaphore))
            thread.start()
    except KeyboardInterrupt:
        print("\n[STOPPING] Server is shutting down.")
    finally:
        server.close()


if __name__ == "__main__":
    start_server()
