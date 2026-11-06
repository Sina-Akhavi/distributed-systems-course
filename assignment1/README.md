# Distributed Banking Server

**GitHub Repository:** [Sina-Akhavi/distributed-systems-course](https://github.com/Sina-Akhavi/distributed-systems-course)

A concurrent, distributed banking server and client architecture demonstrating network communication, multi-threading, shared state management, and ACID compliance using TCP sockets and SQLite.

## Prerequisites
This project uses standard Python libraries. No external dependencies or `pip install` commands are required.
- Python 3.x

## Configuration
The server configuration is managed via a `.env` file. Create a `.env` file in the same directory as `server.py` with the following variables:
```env
HOST=0.0.0.0
PORT=65432
DB_FILE=bank.db
MAX_CLIENTS=10
```

## Running the Server
Start the server from your server machine/VM. You can optionally override the maximum number of concurrent clients using the `--max-clients` argument.

```bash
# Run with default limits from .env
python3 server.py

# Run with a custom limit of 5 concurrent clients
python3 server.py --max-clients 5
```

## Running the Client
The client connects to the server to perform transactions. Ensure the client script points to the correct server IP address before running.

**Check Balance:**
```bash
python3 client.py balance --account A1001
```

**Deposit Funds:**
```bash
python3 client.py deposit --account A1001 --amount 500
```

**Withdraw Funds:**
```bash
python3 client.py withdraw --account A1001 --amount 200
```

## Testing Concurrency & ACID Properties
To demonstrate isolation and concurrency, you can simulate multiple clients attempting to withdraw funds at the exact same time using a bash loop. 

```bash
for i in {1..10}; do python3 client.py withdraw --account A1001 --amount 10 & done; wait
```
*Because the server uses SQLite's `BEGIN IMMEDIATE` exclusive locking and multi-threading with `threading.Semaphore`, all transactions are queued safely without any lost updates or race conditions.*

## Logs
The server automatically generates a `server.log` file in the root directory. It records the timestamp, request ID, client IP, action, resulting balance, and latency for every transaction.