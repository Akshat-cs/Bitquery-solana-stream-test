"""
Solana DEXTrades stream vs query comparison.
Run the stream; press Ctrl+C to stop. Then we fetch the same time range via query
and report trades that were in the query but not in the stream (subscription misses).
"""
import asyncio
import csv
import os
import signal
import sys
import time
from dataclasses import dataclass, field

import requests
from dotenv import load_dotenv
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport
from urllib3.exceptions import ProtocolError as Urllib3ProtocolError

load_dotenv()

STREAM_CSV = "stream_trades.csv"
QUERY_CSV = "query_trades.csv"
MISMATCHES_CSV = "mismatches.csv"
QUERY_LIMIT = 25_000
CSV_COLUMNS = ["blocktime", "signature", "txn_index", "trade_index"]
QUERY_TIMEOUT = 120  # seconds for large responses
QUERY_MAX_RETRIES = 4
QUERY_RETRY_DELAY = 5  # seconds between retries


def _at_time_boundary(blocktime: str, since: str, till: str) -> bool:
    """True if blocktime is at opening (since) or closing (till) boundary.
    Subscription sends trades in random order, so boundary trades may be false-positive mismatches."""
    if not blocktime or not since or not till:
        return False
    # Normalize to second precision (strip Z and optional fractional seconds)
    def to_sec(s: str) -> str:
        s = str(s).replace("Z", "").strip()
        return s[:19] if len(s) >= 19 else s  # YYYY-MM-DDTHH:MM:SS
    bt_sec = to_sec(blocktime)
    return bt_sec == to_sec(since) or bt_sec == to_sec(till)


@dataclass
class StreamState:
    """State collected during stream: CSV file handle and time range."""
    min_time: str | None = None
    max_time: str | None = None
    count: int = 0
    _file: object = field(default=None, repr=False)
    _writer: object = field(default=None, repr=False)

    def open_csv(self, path: str):
        self._file = open(path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=CSV_COLUMNS)
        self._writer.writeheader()
        self._file.flush()

    def write_trade(self, blocktime: str, signature: str, txn_index: int, trade_index: int):
        self._writer.writerow({
            "blocktime": blocktime,
            "signature": signature,
            "txn_index": txn_index,
            "trade_index": trade_index,
        })
        self._file.flush()
        self.count += 1
        if self.min_time is None or blocktime < self.min_time:
            self.min_time = blocktime
        if self.max_time is None or blocktime > self.max_time:
            self.max_time = blocktime

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None
            self._writer = None


def parse_trades_from_result(result) -> list[dict]:
    """Extract list of trade records from a subscription or query result.
    Accepts gql ExecutionResult (has .data) or a dict."""
    try:
        # gql subscribe() yields ExecutionResult objects with .data, not raw dicts
        if hasattr(result, "data"):
            data = result.data
        elif isinstance(result, dict):
            if "payload" in result:
                result = result["payload"]
            data = result.get("data") or result
        else:
            return []
        if not isinstance(data, dict):
            return []
        solana = data.get("Solana")
        if not solana:
            return []
        trades = solana.get("DEXTrades")
        if trades is None:
            return []
        if not isinstance(trades, list):
            trades = [trades]
        out = []
        for t in trades:
            block = t.get("Block") or {}
            tx = t.get("Transaction") or {}
            trade = t.get("Trade") or {}
            blocktime = block.get("Time") or ""
            signature = tx.get("Signature") or ""
            txn_index = tx.get("Index")
            trade_index = trade.get("Index")
            if blocktime and signature is not None:
                out.append({
                    "blocktime": blocktime,
                    "signature": str(signature),
                    "txn_index": txn_index if txn_index is not None else "",
                    "trade_index": trade_index if trade_index is not None else "",
                })
        return out
    except Exception:
        return []


async def subscribe_and_collect(transport: WebsocketsTransport, state: StreamState) -> None:
    """Run Solana DEXTrades subscription and write each trade to state CSV."""
    query = gql("""
        subscription {
          Solana {
            DEXTrades {
              Block {
                Time
                Slot
              }
              Transaction {
                Signature
                Index
                Result {
                  Success
                }
              }
              Trade {
                Index
              }
            }
          }
        }
    """)
    async for result in transport.subscribe(query):
        for row in parse_trades_from_result(result):
            state.write_trade(
                row["blocktime"],
                row["signature"],
                row.get("txn_index") or 0,
                row.get("trade_index") or 0,
            )


async def run_stream(token: str) -> tuple[StreamState, float]:
    """Run stream until Ctrl+C; return (state with CSV and time range, wall_seconds)."""
    stream_start_wall = time.time()
    url = f"wss://streaming.bitquery.io/graphql?token={token}"
    transport = WebsocketsTransport(
        url=url,
        headers={"Sec-WebSocket-Protocol": "graphql-ws"},
    )
    await transport.connect()
    print("Stream connected. Collecting Solana DEXTrades. Press Ctrl+C to stop.\n")

    state = StreamState()
    state.open_csv(STREAM_CSV)
    task = asyncio.create_task(subscribe_and_collect(transport, state))

    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, lambda: task.cancel())
    except NotImplementedError:
        pass  # Windows

    try:
        await task
    except asyncio.CancelledError:
        print("\nStopping stream (Ctrl+C)...")
    finally:
        try:
            loop.remove_signal_handler(signal.SIGINT)
        except NotImplementedError:
            pass
    state.close()
    await transport.close()
    wall_seconds = time.time() - stream_start_wall
    print("Stream closed.")
    return state, wall_seconds


def run_query_and_compare(token: str, state: StreamState, stream_wall_seconds: float) -> None:
    """Fetch all trades in state's time range via query (paginated), compare to stream CSV."""
    since = state.min_time
    till = state.max_time
    if not since or not till:
        print("No time range from stream (no trades collected). Skipping query.")
        return

    print(f"\nQuery time range: since={since} till={till} (UTC)")
    print("Loading stream trades for comparison...")
    stream_keys = set()
    with open(STREAM_CSV, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            key = (row["signature"], str(row.get("txn_index", "")), str(row.get("trade_index", "")))
            stream_keys.add(key)
    stream_count = len(stream_keys)
    print(f"Stream trades loaded: {stream_count}")

    url = f"https://streaming.bitquery.io/graphql?token={token}"
    transport = RequestsHTTPTransport(url=url, timeout=QUERY_TIMEOUT, retries=2)
    client = Client(transport=transport, fetch_schema_from_transport=False)

    query = gql("""
        query MyQuery($since: DateTime!, $till: DateTime!, $offset: Int) {
          Solana {
            DEXTrades(
              limit: { count: 25000, offset: $offset }
              orderBy: { ascending: Block_Time }
              where: { Block: { Time: { since: $since, till: $till } } }
            ) {
              Block { Time Slot }
              Transaction { Signature Index Result { Success } }
              Trade { Index }
            }
          }
        }
    """)

    mismatch_count = 0
    boundary_excluded_count = 0
    total_query_count = 0
    offset = 0
    first_result_time = None
    last_result_time = None

    with open(QUERY_CSV, "w", newline="", encoding="utf-8") as query_file, open(
        MISMATCHES_CSV, "w", newline="", encoding="utf-8"
    ) as mismatch_file:
        query_writer = csv.DictWriter(query_file, fieldnames=CSV_COLUMNS)
        query_writer.writeheader()
        mismatch_writer = csv.DictWriter(mismatch_file, fieldnames=CSV_COLUMNS)
        mismatch_writer.writeheader()

        while True:
            variables = {"since": since, "till": till, "offset": offset}
            last_error = None
            for attempt in range(QUERY_MAX_RETRIES):
                try:
                    result = client.execute(query, variable_values=variables)
                    break
                except (
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    Urllib3ProtocolError,
                ) as e:
                    last_error = e
                    if attempt < QUERY_MAX_RETRIES - 1:
                        wait = QUERY_RETRY_DELAY * (attempt + 1)
                        print(f"  Query offset {offset}: connection error, retrying in {wait}s ({attempt + 1}/{QUERY_MAX_RETRIES})...")
                        time.sleep(wait)
                    else:
                        raise
            rows = parse_trades_from_result(result)
            if not rows:
                break
            for row in rows:
                total_query_count += 1
                bt = row["blocktime"]
                if first_result_time is None:
                    first_result_time = bt
                last_result_time = bt
                row_dict = {
                    "blocktime": row["blocktime"],
                    "signature": row["signature"],
                    "txn_index": row.get("txn_index", ""),
                    "trade_index": row.get("trade_index", ""),
                }
                query_writer.writerow(row_dict)
                key = (row["signature"], str(row.get("txn_index", "")), str(row.get("trade_index", "")))
                if key not in stream_keys:
                    if _at_time_boundary(bt, since, till):
                        boundary_excluded_count += 1
                    else:
                        mismatch_count += 1
                        mismatch_writer.writerow(row_dict)
            print(f"  Query offset {offset}: got {len(rows)} trades (total query so far: {total_query_count}, mismatches: {mismatch_count}, boundary excluded: {boundary_excluded_count})")
            if len(rows) < QUERY_LIMIT:
                break
            offset += QUERY_LIMIT
            time.sleep(2)  # brief pause between pages to avoid connection drops

    # Report stats
    print("\n" + "=" * 60)
    print("FINAL STATS")
    print("=" * 60)
    print(f"Stream wall-clock duration: {stream_wall_seconds:.1f} seconds")
    print(f"Stream time range (UTC): since = {since}, till = {till}")
    print(f"Count of trades received in stream: {state.count}")
    print(f"Count of trades from query in same time range: {total_query_count}")
    print(f"Excluded as boundary (opening/closing timestamp, random order): {boundary_excluded_count}")
    print(f"Mismatches (in query but NOT in subscription): {mismatch_count}")
    if total_query_count > 0:
        pct = 100.0 * mismatch_count / total_query_count
        print(f"Missing share: {pct:.2f}% of query trades were not in stream")
    print(f"Query trades written to: {QUERY_CSV}")
    print(f"Mismatch rows written to: {MISMATCHES_CSV}")
    print("=" * 60)


def main():
    token = os.getenv("BITQUERY_TOKEN", "").strip()
    if not token:
        print("Missing BITQUERY_TOKEN in .env. Copy .env.example to .env and set your token.", file=sys.stderr)
        sys.exit(1)

    state, stream_wall_seconds = asyncio.run(run_stream(token))
    if state.count == 0:
        print("No trades collected from stream. Nothing to compare.")
        return
    run_query_and_compare(token, state, stream_wall_seconds)


if __name__ == "__main__":
    main()
