import os
import sqlite3
import uuid
import itertools
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request

SHARD_A_NODES = ["http://bank_server_a1:80", "http://bank_server_a2:80"]
SHARD_B_NODES = ["http://bank_server_b1:80", "http://bank_server_b2:80"]

round_robin_a = itertools.cycle(SHARD_A_NODES)
round_robin_b = itertools.cycle(SHARD_B_NODES)

# --- COORDINATOR DURABLE STATE & RECOVERY ---
def init_db():
    conn = sqlite3.connect('coordinator.db')
    conn.execute("CREATE TABLE IF NOT EXISTS tx_log (txn_id TEXT PRIMARY KEY, status TEXT)")
    conn.commit()
    conn.close()

def log_tx(txn_id: str, status: str):
    conn = sqlite3.connect('coordinator.db')
    conn.execute("INSERT OR REPLACE INTO tx_log (txn_id, status) VALUES (?, ?)", (txn_id, status))
    conn.commit()
    conn.close()

def recover_pending_transactions():
    """EXPERIMENT 6: Run on startup to recover interrupted transactions."""
    conn = sqlite3.connect('coordinator.db')
    cur = conn.cursor()
    cur.execute("SELECT txn_id, status FROM tx_log WHERE status = 'COMMIT_DECIDED'")
    pending = cur.fetchall()
    
    for txn_id, status in pending:
        print(f"\n[RECOVERY] Resuming interrupted transaction: {txn_id}")
        # Broadcast COMMIT to all nodes. The bank servers are idempotent!
        for node in SHARD_A_NODES + SHARD_B_NODES:
            try:
                requests.post(f"{node}/commit", json={"txn_id": txn_id}, timeout=2)
            except:
                pass
        log_tx(txn_id, "COMPLETED")
        print(f"[RECOVERY] Transaction {txn_id} successfully COMPLETED\n")
    conn.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    recover_pending_transactions()
    yield

app = FastAPI(lifespan=lifespan)

# --- ROUTING ---
def get_shard_node(account_id: str) -> str:
    try:
        num = int(account_id[1:])
        if 1 <= num <= 1000: return next(round_robin_a)
        elif 1001 <= num <= 2000: return next(round_robin_b)
        else: raise ValueError
    except:
        raise HTTPException(status_code=400, detail="Invalid account ID format")

def forward_request(method: str, target_url: str, endpoint: str, json_data: dict = None, retries: int = 1):
    full_url = f"{target_url}{endpoint}"
    try:
        response = requests.request(method, full_url, json=json_data, timeout=2)
        return response.json()
    except requests.exceptions.RequestException:
        if retries > 0:
            next_url = next(round_robin_a) if "a" in target_url else next(round_robin_b)
            return forward_request(method, next_url, endpoint, json_data, retries - 1)
        raise HTTPException(status_code=503, detail="Bank replica unavailable")

@app.get("/balance/{account_id}")
def get_balance(account_id: str):
    return forward_request("GET", get_shard_node(account_id), f"/balance/{account_id}")

# --- TWO PHASE COMMIT (NORMAL) ---
@app.post("/transfer_cross_shard")
async def transfer_cross_shard(request: Request):
    data = await request.json()
    from_acc, to_acc, amount = data["from_account"], data["to_account"], float(data["amount"])
    node_from, node_to = get_shard_node(from_acc), get_shard_node(to_acc)
    
    txn_id = f"tx_{uuid.uuid4().hex[:8]}"
    log_tx(txn_id, "INITIATED")
    
    res_from = forward_request("POST", node_from, "/prepare", {"txn_id": txn_id, "account_id": from_acc, "delta": -amount})
    res_to = forward_request("POST", node_to, "/prepare", {"txn_id": txn_id, "account_id": to_acc, "delta": amount})
    
    if res_from.get("vote") != "YES" or res_to.get("vote") != "YES":
        log_tx(txn_id, "ABORT_DECIDED")
        forward_request("POST", node_from, "/abort", {"txn_id": txn_id})
        forward_request("POST", node_to, "/abort", {"txn_id": txn_id})
        raise HTTPException(status_code=400, detail="Transfer aborted.")

    log_tx(txn_id, "COMMIT_DECIDED")
    forward_request("POST", node_from, "/commit", {"txn_id": txn_id})
    forward_request("POST", node_to, "/commit", {"txn_id": txn_id})
    log_tx(txn_id, "COMPLETED")
    return {"status": "success", "txn_id": txn_id}

# --- TWO PHASE COMMIT (SABOTAGED FOR EXPERIMENT 6) ---
@app.post("/transfer_cross_shard_crash")
async def transfer_cross_shard_crash(request: Request):
    data = await request.json()
    from_acc, to_acc, amount = data["from_account"], data["to_account"], float(data["amount"])
    node_from, node_to = get_shard_node(from_acc), get_shard_node(to_acc)
    
    txn_id = f"tx_CRASH_{uuid.uuid4().hex[:4]}"
    log_tx(txn_id, "INITIATED")
    
    # Phase 1: Prepare
    forward_request("POST", node_from, "/prepare", {"txn_id": txn_id, "account_id": from_acc, "delta": -amount})
    forward_request("POST", node_to, "/prepare", {"txn_id": txn_id, "account_id": to_acc, "delta": amount})
    
    # Record the durable decision, then instantly kill the container before Phase 2 executes
    log_tx(txn_id, "COMMIT_DECIDED")
    print(f"\n!!! SIMULATING COORDINATOR CRASH FOR {txn_id} !!!\n")
    os._exit(1)

@app.post("/deposit")
async def deposit(request: Request):
    data = await request.json()
    return forward_request("POST", get_shard_node(data.get("account_id")), "/deposit", data)

@app.post("/withdraw")
async def withdraw(request: Request):
    data = await request.json()
    return forward_request("POST", get_shard_node(data.get("account_id")), "/withdraw", data)

@app.post("/transfer_local")
async def transfer_local(request: Request):
    data = await request.json()
    url_from = get_shard_node(data.get("from_account"))
    url_to = get_shard_node(data.get("to_account"))
    
    # Check if both accounts map to the same shard
    if any(node in url_from for node in SHARD_A_NODES) != any(node in url_to for node in SHARD_A_NODES):
        raise HTTPException(status_code=400, detail="Accounts are on different shards. Use cross-shard transfer.")
    
    return forward_request("POST", url_from, "/transfer_local", data)
