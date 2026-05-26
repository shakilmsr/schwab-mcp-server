# Schwab Market Data MCP Server

A high-performance MCP (Model Context Protocol) server that streams real-time market data from Charles Schwab's WebSocket API into any MCP-compatible client (Claude Desktop, Claude Code, etc.).

Features **live Level 1 quotes, 1-minute candles, and automatic token refresh** with in-memory buffering for instant data retrieval.

---

## Prerequisites

- **Python 3.11+**
- **Charles Schwab developer account** with:
  - A registered developer app
  - **Market Data Production** enabled
  - Callback URL set to `https://127.0.0.1`
- An **active Schwab brokerage account** (paper or live trading)

Register at: https://developer.schwab.com

---

## Installation

### 1. Install Dependencies

```bash
cd schwab-mcp-server
pip install -r requirements.txt
```

Required packages:
- `schwab-py>=1.4.0` — Schwab API client with WebSocket support
- `mcp>=1.0.0` — Model Context Protocol server
- `httpx>=0.27.0` — HTTP client
- `python-dotenv>=1.0.0` — Environment variable loader

### 2. Set Environment Variables

Create a `credentials.env` file in the project directory:

```bash
SCHWAB_APP_KEY="your_app_key_here"
SCHWAB_APP_SECRET="your_app_secret_here"
SCHWAB_CALLBACK_URL="https://127.0.0.1"
SCHWAB_TOKEN_PATH="token.json"
SCHWAB_BUFFER_SIZE="500"   # Optional: quotes kept in memory per symbol
```

Or set them in your shell before running the server.

### 3. Authenticate (One-Time)

```bash
python get_token.py
```

A browser window opens automatically. Log into Schwab, authorize the app, and paste the redirect URL back into the terminal. The script writes `token.json` and exits.

From then on, the server automatically refreshes your token every 24 hours.

---

## Usage

### Start the Server Directly

```bash
python server.py
```

The server listens on stdin/stdout for JSON-RPC messages (standard MCP protocol).

### Register with Claude Code

```bash
claude mcp add schwab-market-data \
  -e SCHWAB_APP_KEY="your_key" \
  -e SCHWAB_APP_SECRET="your_secret" \
  -e SCHWAB_TOKEN_PATH="C:\path\to\token.json" \
  -- python C:\path\to\schwab-mcp-server\server.py
```

Then ask Claude to stream market data. Examples:

> "Subscribe to real-time quotes for AAPL and NVDA"

> "Show me the last 20 quotes for TSLA"

> "Subscribe to 1-minute candles for SPY"

### Register with Claude Desktop

Edit `%APPDATA%\Claude\claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "schwab-market-data": {
      "command": "python",
      "args": ["C:\\absolute\\path\\to\\server.py"],
      "env": {
        "SCHWAB_APP_KEY": "your_app_key",
        "SCHWAB_APP_SECRET": "your_app_secret",
        "SCHWAB_CALLBACK_URL": "https://127.0.0.1",
        "SCHWAB_TOKEN_PATH": "C:\\absolute\\path\\to\\token.json"
      }
    }
  }
}
```

Restart Claude Desktop. The server starts on demand when you use a tool.

---

## Available Tools

### 1. `subscribe_quotes`

Stream real-time Level 1 equity quotes (bid, ask, last, volume, OHLC, net change).

**Parameters:**
- `symbols` (array of strings, required): Ticker symbols to subscribe to (e.g., `["AAPL", "TSLA"]`)

**Example:**
```
subscribe_quotes(symbols=["AAPL", "MSFT", "GOOGL"])
```

Data flows into the in-memory buffer. Retrieve it with `get_latest_quotes`.

---

### 2. `subscribe_chart`

Stream real-time 1-minute OHLCV (Open, High, Low, Close, Volume) candles.

**Parameters:**
- `symbols` (array of strings, required): Ticker symbols (e.g., `["SPY", "QQQ"]`)

**Example:**
```
subscribe_chart(symbols=["SPY", "QQQ"])
```

Candle data updates every minute during market hours.

---

### 3. `get_latest_quotes`

Retrieve the most recent buffered quotes or candles for a symbol.

**Parameters:**
- `symbol` (string, required): Ticker symbol (e.g., `"AAPL"`)
- `n` (integer, optional, default=10): Number of recent data points to return (max 500)
- `type` (string, optional, default="quote"): `"quote"` for Level 1 quotes or `"chart"` for OHLCV candles

**Example:**
```
get_latest_quotes(symbol="AAPL", n=20, type="quote")
```

Returns JSON array of up to N most recent ticks, newest first.

---

### 4. `list_subscriptions`

Show all currently active streaming subscriptions and buffer sizes.

**Parameters:** None

**Example:**
```
list_subscriptions()
```

**Output:**
```json
{
  "level1_quotes": ["AAPL", "MSFT"],
  "chart_candles": ["SPY"],
  "buffer_sizes": {
    "quotes": {"AAPL": 45, "MSFT": 23},
    "charts": {"SPY": 52}
  }
}
```

---

### 5. `unsubscribe`

Stop streaming for given symbols.

**Parameters:**
- `symbols` (array of strings, required): Symbols to unsubscribe from
- `type` (string, optional, default="both"): `"quote"`, `"chart"`, or `"both"`

**Example:**
```
unsubscribe(symbols=["AAPL"], type="quote")
```

---

## Architecture

```
Client (Claude Desktop / Claude Code)
    ↓ JSON-RPC over stdio
MCP Server (server.py)
    ├─ schwab.AsyncClient (HTTP + OAuth)
    ├─ schwab.StreamClient (WebSocket)
    │   ├─ Quote Stream Handler
    │   └─ Chart Stream Handler
    ├─ In-Memory Buffers
    │   ├─ quote_buffer: {symbol → deque(500)}
    │   └─ chart_buffer: {symbol → deque(500)}
    └─ Background Tasks
        ├─ handle_message() loop
        └─ Token refresh (24-hour cycle)
```

**Data Flow:**
1. Client subscribes to symbols via MCP tool
2. Server calls `StreamClient.level_one_equity_subs()` or `chart_equity_subs()`
3. Schwab WebSocket sends data continuously during market hours
4. Handler functions append data to in-memory deques (newest first)
5. Client retrieves data with `get_latest_quotes()` or accesses the stream directly

---

## Example Usage

### Stream AAPL Quotes

```
1. subscribe_quotes(symbols=["AAPL"])
   → "Subscribed to Level 1 quotes for: ['AAPL']"

2. (Wait a few seconds for data to arrive)

3. get_latest_quotes(symbol="AAPL", n=5, type="quote")
   → [
       {"ts": "2026-05-26T21:45:32.123456", "symbol": "AAPL", "bid": 189.50, "ask": 189.52, "last": 189.51, ...},
       {"ts": "2026-05-26T21:45:31.987654", "symbol": "AAPL", "bid": 189.48, "ask": 189.50, "last": 189.49, ...},
       ...
     ]

4. unsubscribe(symbols=["AAPL"], type="quote")
   → "Unsubscribed: ['AAPL (quote)']"
```

### Track Multiple Symbols

```
subscribe_quotes(symbols=["AAPL", "MSFT", "GOOGL"])
subscribe_chart(symbols=["SPY", "QQQ"])

list_subscriptions()
→ Shows all 5 active subscriptions

get_latest_quotes(symbol="SPY", n=10, type="chart")
→ Returns 10 most recent 1-minute candles
```

---

## Data Format

### Quote Message

```json
{
  "ts": "2026-05-26T21:45:32.123456Z",
  "symbol": "AAPL",
  "bid": 189.50,
  "ask": 189.52,
  "last": 189.51,
  "volume": 52341000,
  "open": 188.75,
  "high": 190.25,
  "low": 188.50,
  "close": 189.51,
  "net_change": 0.76,
  "pct_change": 0.40
}
```

### Chart Message (1-Minute Candle)

```json
{
  "ts": "2026-05-26T21:45:00.000000Z",
  "symbol": "SPY",
  "open": 529.25,
  "high": 529.75,
  "low": 529.10,
  "close": 529.50,
  "volume": 1234567,
  "chart_time": 1748274300000,
  "seq": 12345
}
```

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SCHWAB_APP_KEY` | Yes | — | Your Schwab app key |
| `SCHWAB_APP_SECRET` | Yes | — | Your Schwab app secret |
| `SCHWAB_CALLBACK_URL` | No | `https://127.0.0.1` | Must match your Schwab app settings |
| `SCHWAB_TOKEN_PATH` | No | `token.json` | Path to OAuth token file |
| `SCHWAB_BUFFER_SIZE` | No | `500` | Quotes/candles buffered per symbol |

### Token Refresh

The server automatically refreshes your OAuth token every 24 hours via a background asyncio task. No manual action needed.

If the token expires unexpectedly, run:
```bash
python refresh_token.py
```

Or delete `token.json` and re-run `python get_token.py` to re-authenticate.

---

## Market Hours & Data Availability

- **Regular Hours:** 9:30 AM – 4:00 PM ET (Mon–Fri, market days)
- **Extended Hours:** Depending on your account type, you may see pre-market and after-hours data
- **Data Only Flows During Trading Hours:** Outside market hours, the stream may be idle or unavailable

---

## Troubleshooting

### "Timed out connecting to Schwab stream"
- Check your credentials in `credentials.env`
- Verify Market Data Production is enabled in your Schwab app
- Ensure your Schwab account is active and not locked
- Check internet connectivity

### "Token file not found"
```bash
python get_token.py
```
This generates a fresh token via OAuth.

### "No data buffered for SYMBOL yet"
- Ensure you've subscribed to that symbol via `subscribe_quotes()` or `subscribe_chart()`
- Check that the market is currently open (9:30 AM – 4:00 PM ET)
- Wait a few seconds for the first data to arrive
- Verify the symbol is valid (e.g., `AAPL`, not `Apple`)

### Server exits immediately
Check logs and run with verbose output:
```bash
python -u server.py 2>&1 | tee server.log
```

### "Connection refused" or "Network error"
- Verify internet connectivity
- Check firewall rules (the server needs outbound HTTPS and WebSocket)
- Ensure Schwab's API servers are not experiencing an outage

---

## Performance & Limitations

- **Buffer Size:** 500 quotes/candles per symbol (configurable)
- **Latency:** ~10–500 ms from Schwab WebSocket to client (depending on network)
- **Symbols:** No theoretical limit, but performance degrades with many active subscriptions
- **Memory:** ~50 KB per symbol (500-item buffer)

---

## Security Notes

- **Never commit credentials:** `credentials.env` and `token.json` should be in `.gitignore`
- **Token file permissions:** Protect `token.json` (contains OAuth refresh token)
- **OAuth scope:** The token is read-only for market data; it cannot place trades

---

## License & Attribution

Built with:
- [schwab-py](https://github.com/alexgolec/schwab-py) — Schwab API wrapper
- [mcp](https://github.com/modelcontextprotocol/python-sdk) — Model Context Protocol
- [FastMCP](https://github.com/jlowin/fastmcp) — FastMCP framework (alternative implementation)

---

## Support & Feedback

For issues or questions:
1. Check the troubleshooting section above
2. Review `server.py` comments for implementation details
3. Run `python get_token.py` to refresh credentials
4. Check Schwab's API status and documentation at https://developer.schwab.com

---

## Changelog

### v1.0.0 (2026-05-26)
- Initial release
- WebSocket streaming for quotes and chart candles
- In-memory circular buffers (500 items per symbol)
- Automatic 24-hour token refresh
- 5 MCP tools: subscribe_quotes, subscribe_chart, get_latest_quotes, list_subscriptions, unsubscribe
- Tested with Python 3.13, schwab-py 1.5.1, mcp 1.27.1
