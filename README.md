# Bitquery Solana DEX Trades Stream Test

A small script to **test whether the Bitquery Solana DEX Trades stream is running fine without dropping any trades or not**. It records trades from the live subscription, then compares them to the same time range fetched via the GraphQL query API and reports any gaps.

## What it does

1. **Stream phase** – Connects to the Solana DEXTrades WebSocket subscription and writes every trade to `stream_trades.csv` (blocktime, signature, txn_index, trade_index).
2. **Stop** – You stop the stream with **Ctrl+C**. The script uses the first and last trade timestamps from the stream as the time window.
3. **Query phase** – Fetches all trades in that window from the query API (paginated by 25k) and writes them to `query_trades.csv`.
4. **Compare** – Finds trades that appear in the query but **not** in the stream and writes them to `mismatches.csv`, then prints summary stats.

## Important: random order and what we actually check

**The subscription sends trades in random order.** Solana blocks are ~400ms and a lot of data is streamed, so the order of messages is not guaranteed.

We only care about **trades missed in between the start and end timestamps**. Trades that fall exactly on the **opening** or **closing** timestamp are **not** counted as mismatches, because they can be missing from the stream purely due to ordering/timing at the boundary, not because the stream dropped them. So the mismatch report is about trades in the middle of the window that the stream never delivered.

## How to run

1. **Clone** (or download) this repo.

2. **Create `.env`** with your Bitquery token:

   ```bash
   cp .env.example .env
   ```

   Then edit `.env` and set:

   ```
   BITQUERY_TOKEN=your_actual_token
   ```

   Get the token from [Bitquery](https://account.bitquery.io/user/api_v2/access_tokens). Do not commit `.env`.

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Run:**
   ```bash
   python stream_compare.py
   ```
   Let it run for as long as you want (e.g. 1–5 minutes), then press **Ctrl+C** to stop the stream. The script will run the query, compare, and print stats.

## Output files

| File                | Contents                                                                      |
| ------------------- | ----------------------------------------------------------------------------- |
| `stream_trades.csv` | Trades received from the subscription                                         |
| `query_trades.csv`  | Trades returned by the query for the same time range                          |
| `mismatches.csv`    | Trades in the query but **not** in the stream (excluding boundary timestamps) |

## Stats printed

- Stream wall-clock duration and time range (UTC)
- Count of trades from stream vs query
- Count excluded as boundary (opening/closing timestamp)
- **Mismatch count** – trades in the middle of the window that the stream did not deliver
- Missing share (%)

## Requirements

- Python 3.10+
- Bitquery API token (Streaming/GraphQL access)
