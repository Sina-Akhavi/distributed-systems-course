import os
import re
import psycopg2
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_HOST = os.getenv("DB_HOST", "db_a")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "bank_db")
SHARD_NAME = os.getenv("SHARD_NAME", "Shard_A")
REPLICA_NAME = os.getenv("REPLICA_NAME", "Unknown")

GID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")   # safe for PREPARE TRANSACTION literal


def get_db_connection(autocommit: bool = False):
    conn = psycopg2.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )
    conn.autocommit = autocommit
    return conn


def make_gid(txn_id: str) -> str:
    gid = f"{txn_id}:{SHARD_NAME}"
    if not GID_RE.match(gid):
        raise HTTPException(status_code=400, detail="Invalid txn_id format")
    return gid


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Durable participant record so /commit and /abort are idempotent across restarts.
    conn = get_db_connection(autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS participant_txns (
            gid        TEXT PRIMARY KEY,
            status     TEXT NOT NULL,           -- PREPARED | COMMITTED | ABORTED
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    cur.close()
    conn.close()
    yield


app = FastAPI(lifespan=lifespan)


# ---------- models ----------
class AmountRequest(BaseModel):
    account_id: str
    amount: float

class TransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: float

class PrepareRequest(BaseModel):
    txn_id: str
    account_id: str
    delta: float          # negative = debit (withdraw side), positive = credit (deposit side)

class TxnRequest(BaseModel):
    txn_id: str


# ---------- health (used by gateway for failover, Step 4) ----------
@app.get("/health")
def health():
    return {"ok": True, "shard": SHARD_NAME, "replica": REPLICA_NAME}


# ---------- existing local operations (unchanged) ----------
@app.get("/balance/{account_id}")
def get_balance(account_id: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT balance FROM accounts WHERE id = %s", (account_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    if not result:
        raise HTTPException(status_code=404, detail="Account not found in this shard")
    return {"shard": SHARD_NAME, "replica": REPLICA_NAME,
            "account_id": account_id, "balance": float(result[0])}


@app.post("/deposit")
def deposit(req: AmountRequest):
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT balance FROM accounts WHERE id = %s FOR UPDATE", (req.account_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Account not found")
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (req.amount, req.account_id))
        conn.commit()
        return {"status": "success", "message": f"Deposited {req.amount}"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cur.close(); conn.close()


@app.post("/withdraw")
def withdraw(req: AmountRequest):
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT balance FROM accounts WHERE id = %s FOR UPDATE", (req.account_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Account not found")
        if row[0] < req.amount:
            raise HTTPException(status_code=400, detail="Insufficient funds")
        cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s",
                    (req.amount, req.account_id))
        conn.commit()
        return {"status": "success", "message": f"Withdrew {req.amount}"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cur.close(); conn.close()


@app.post("/transfer_local")
def transfer_local(req: TransferRequest):
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        accounts = sorted([req.from_account, req.to_account])
        cur.execute("SELECT id, balance FROM accounts WHERE id IN %s FOR UPDATE",
                    (tuple(accounts),))
        rows = cur.fetchall()
        if len(rows) != 2:
            raise HTTPException(status_code=404, detail="One or both accounts not found in this shard")
        balances = {row[0]: row[1] for row in rows}
        if balances[req.from_account] < req.amount:
            raise HTTPException(status_code=400, detail="Insufficient funds")
        cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s",
                    (req.amount, req.from_account))
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (req.amount, req.to_account))
        conn.commit()
        return {"status": "success", "message": "Local transfer complete"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cur.close(); conn.close()


# ================= 2PC PARTICIPANT ENDPOINTS =================

@app.post("/prepare")
def prepare(req: PrepareRequest):
    """Phase 1. Validate + durably prepare (row stays locked, uncommitted) and vote."""
    if req.delta == 0:
        raise HTTPException(status_code=400, detail="delta must be non-zero")
    gid = make_gid(req.txn_id)

    # Idempotent re-prepare
    conn = get_db_connection(autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT status FROM participant_txns WHERE gid = %s", (gid,))
    rec = cur.fetchone()
    if rec:
        cur.close(); conn.close()
        if rec[0] in ("PREPARED", "COMMITTED"):
            return {"vote": "YES", "gid": gid, "note": "already prepared/committed"}
        return {"vote": "NO", "gid": gid, "reason": "already aborted"}

    try:
        cur.execute("BEGIN")
        cur.execute("SELECT balance FROM accounts WHERE id = %s FOR UPDATE", (req.account_id,))
        row = cur.fetchone()
        if not row:
            cur.execute("ROLLBACK")
            return {"vote": "NO", "gid": gid, "reason": "account not found"}
        if req.delta < 0 and row[0] < -req.delta:
            cur.execute("ROLLBACK")
            return {"vote": "NO", "gid": gid, "reason": "insufficient funds"}

        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s",
                    (req.delta, req.account_id))
        # autocommit=True + explicit BEGIN => PREPARE TRANSACTION is legal here
        cur.execute(f"PREPARE TRANSACTION '{gid}'")

        # Durable vote record (separate autocommit statement)
        cur.execute("""
            INSERT INTO participant_txns (gid, status) VALUES (%s, 'PREPARED')
            ON CONFLICT (gid) DO UPDATE SET status='PREPARED', updated_at=now()
        """, (gid,))
        return {"vote": "YES", "gid": gid}
    except Exception as e:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        return {"vote": "NO", "gid": gid, "reason": f"prepare failed: {e}"}
    finally:
        cur.close(); conn.close()


@app.post("/commit")
def commit(req: TxnRequest):
    """Phase 2 COMMIT. Idempotent; NEVER downgrades a recorded commit."""
    gid = make_gid(req.txn_id)
    conn = get_db_connection(autocommit=True)
    cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM participant_txns WHERE gid = %s", (gid,))
        rec = cur.fetchone()
        if rec and rec[0] == "COMMITTED":
            return {"status": "COMMITTED", "gid": gid, "note": "idempotent"}

        cur.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
        if cur.fetchone():
            cur.execute(f"COMMIT PREPARED '{gid}'")

        cur.execute("""
            INSERT INTO participant_txns (gid, status) VALUES (%s, 'COMMITTED')
            ON CONFLICT (gid) DO UPDATE SET status='COMMITTED', updated_at=now()
        """, (gid,))
        return {"status": "COMMITTED", "gid": gid,
                "note": "already committed (no prepared xact found)" if not rec else "committed"}
    except Exception as e:
        return {"status": "ERROR", "gid": gid, "reason": str(e)}
    finally:
        cur.close(); conn.close()


@app.post("/abort")
def abort(req: TxnRequest):
    """Phase 2 ABORT. Idempotent; refuses to abort something already COMMITTED."""
    gid = make_gid(req.txn_id)
    conn = get_db_connection(autocommit=True)
    cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM participant_txns WHERE gid = %s", (gid,))
        rec = cur.fetchone()
        if rec and rec[0] == "COMMITTED":
            # Scenario C: a recorded COMMIT must never be replaced by ABORT.
            return {"status": "COMMITTED", "gid": gid,
                    "note": "already committed; abort ignored"}

        cur.execute("SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (gid,))
        if cur.fetchone():
            cur.execute(f"ROLLBACK PREPARED '{gid}'")

        cur.execute("""
            INSERT INTO participant_txns (gid, status) VALUES (%s, 'ABORTED')
            ON CONFLICT (gid) DO UPDATE SET status='ABORTED', updated_at=now()
        """, (gid,))
        return {"status": "ABORTED", "gid": gid}
    except Exception as e:
        return {"status": "ERROR", "gid": gid, "reason": str(e)}
    finally:
        cur.close(); conn.close()


@app.get("/prepared")
def prepared_list():
    """Debug helper: what this shard currently has in-doubt."""
    conn = get_db_connection(autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT gid, prepared FROM pg_prepared_xacts")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return {"shard": SHARD_NAME, "replica": REPLICA_NAME,
            "prepared": [{"gid": r[0], "prepared_at": r[1].isoformat()} for r in rows]}
