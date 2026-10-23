# Distributed Banking System

This project is a concurrent client-server banking application designed to demonstrate core distributed systems concepts. It ensures thread-safe operations, configurable concurrency limits, and strict adherence to ACID (Atomicity, Consistency, Isolation, Durability) database properties.

## Features
* **TCP Socket Communication:** Reliable client-server messaging using JSON payloads.
* **Concurrency Control:** Utilizes Python's `threading.Semaphore` to limit maximum simultaneous connections.
* **ACID Guarantees:** 
  * *Isolation:* Uses SQLite `BEGIN IMMEDIATE` to prevent race conditions and lost updates.
  * *Consistency:* Enforces database-level constraints (e.g., `CHECK(balance >= 0)`).
* **Dynamic Configuration:** Supports `.env` files and command-line arguments.

## Prerequisites
* Python 3.x
* No external dependencies are required (relies only on standard libraries: `socket`, `threading`, `sqlite3`, `json`, `argparse`, `os`).

## Setup & Configuration

1. Clone the repository to both your Server VM and Client VM.
2. On the **Server VM**, create a `.env` file in the project root to define default configurations:

```env
HOST=0.0.0.0
PORT=65432
DB_FILE=bank.db
MAX_CLIENTS=10
```

## Running the Server

Start the server on your Server VM. You can override the `.env` settings using command-line arguments. To start the server with a strict limit of 5 simultaneous clients:

```bash
python3 server.py --max-clients 5
```
*Note: The server will automatically initialize the `bank.db` SQLite database with dummy accounts (A1001 and A1002) upon starting.*

## Running the Client

On your Client VM, use the `client.py` script to interact with the server. Ensure the client script is configured to point to the Server VM's IP address.

### Standard Operations

**1. Check Balance:**
```bash
python3 client.py balance --account A1001
```

**2. Deposit Funds:**
```bash
python3 client.py deposit --account A1001 --amount 500
```

**3. Withdraw Funds:**
```bash
python3 client.py withdraw --account A1001 --amount 200
```

### Testing Error Handling

The system is designed to gracefully handle invalid inputs and constraint violations without crashing:

* **Unknown Account:** `python3 client.py balance --account Z9999`
* **Insufficient Funds:** `python3 client.py withdraw --account A1001 --amount 50000`
* **Negative Amount:** `python3 client.py withdraw --account A1001 --amount -50`
* **Malformed Request:** `python3 client.py malformed`

## Concurrency Testing (Stress Test)

To verify the system's isolation and thread safety (preventing lost updates), run the following bash loop on the Client VM. This fires 10 simultaneous withdrawal requests in the background:

```bash
for i in {1..10}; do python3 client.py withdraw --account A1001 --amount 10 & done; wait
```
After the loop finishes, run a balance check to confirm the math is perfectly accurate and no updates were lost.

## Logs
The server logs all incoming requests, actions, outcomes, and latencies. You can monitor this live by opening a new terminal pane on the Server VM and running:
```bash
tail -f server.log
```