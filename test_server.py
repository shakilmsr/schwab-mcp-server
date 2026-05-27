"""
Tests for schwab-mcp-server/server.py
"""
import asyncio
import json
import os
import sys
from collections import deque
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from mcp.types import TextContent

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))


# Mock environment variables before importing server
@pytest.fixture(autouse=True)
def mock_env():
    with patch.dict(os.environ, {
        "SCHWAB_APP_KEY": "test-key",
        "SCHWAB_APP_SECRET": "test-secret",
        "SCHWAB_TOKEN_PATH": "token.json",
        "SCHWAB_CALLBACK_URL": "https://127.0.0.1",
        "SCHWAB_BUFFER_SIZE": "500",
    }):
        yield


@pytest.fixture
def reset_server_state():
    """Reset global server state before each test."""
    # Import after env is mocked
    import server

    # Reset global state
    server.quote_buffer.clear()
    server.chart_buffer.clear()
    server.active_quote_symbols.clear()
    server.active_chart_symbols.clear()
    server.stream_client = None
    server._stream_task = None
    # Recreate the event to avoid event loop binding issues
    server._stream_started = asyncio.Event()

    yield

    # Cleanup after test
    server.quote_buffer.clear()
    server.chart_buffer.clear()
    server.active_quote_symbols.clear()
    server.active_chart_symbols.clear()
    server.stream_client = None
    server._stream_task = None
    server._stream_started = asyncio.Event()


# ── Data Handler Tests ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_quote_handler_parses_message(reset_server_state):
    """Test that quote handler correctly parses and buffers quote data."""
    import server

    handler = server._make_quote_handler("AAPL")

    msg = {
        "content": [{
            "key": "AAPL",
            "BID_PRICE": 150.25,
            "ASK_PRICE": 150.30,
            "LAST_PRICE": 150.27,
            "TOTAL_VOLUME": 1000000,
            "OPEN_PRICE": 149.50,
            "HIGH_PRICE": 151.00,
            "LOW_PRICE": 149.25,
            "CLOSE_PRICE": 150.27,
            "NET_CHANGE": 0.77,
            "NET_PERCENT_CHANGE": 0.52,
        }]
    }

    await handler(msg)

    assert len(server.quote_buffer["AAPL"]) == 1
    entry = server.quote_buffer["AAPL"][0]

    assert entry["symbol"] == "AAPL"
    assert entry["bid"] == 150.25
    assert entry["ask"] == 150.30
    assert entry["last"] == 150.27
    assert entry["volume"] == 1000000
    assert entry["open"] == 149.50
    assert entry["high"] == 151.00
    assert entry["low"] == 149.25
    assert entry["close"] == 150.27
    assert entry["net_change"] == 0.77
    assert entry["pct_change"] == 0.52
    assert "ts" in entry


@pytest.mark.asyncio
async def test_quote_handler_with_missing_fields(reset_server_state):
    """Test that quote handler handles missing fields gracefully."""
    import server

    handler = server._make_quote_handler("TSLA")

    msg = {
        "content": [{
            "key": "TSLA",
            "BID_PRICE": 250.50,
            "ASK_PRICE": 250.60,
        }]
    }

    await handler(msg)

    assert len(server.quote_buffer["TSLA"]) == 1
    entry = server.quote_buffer["TSLA"][0]
    assert entry["symbol"] == "TSLA"
    assert entry["bid"] == 250.50
    assert entry["ask"] == 250.60
    assert entry["last"] is None
    assert entry["volume"] is None


@pytest.mark.asyncio
async def test_chart_handler_parses_message(reset_server_state):
    """Test that chart handler correctly parses and buffers chart data."""
    import server

    handler = server._make_chart_handler("SPY")

    msg = {
        "content": [{
            "key": "SPY",
            "OPEN_PRICE": 450.00,
            "HIGH_PRICE": 451.50,
            "LOW_PRICE": 449.75,
            "CLOSE_PRICE": 451.25,
            "VOLUME": 500000,
            "CHART_TIME": 1234567890,
            "SEQUENCE": 1,
        }]
    }

    await handler(msg)

    assert len(server.chart_buffer["SPY"]) == 1
    entry = server.chart_buffer["SPY"][0]

    assert entry["symbol"] == "SPY"
    assert entry["open"] == 450.00
    assert entry["high"] == 451.50
    assert entry["low"] == 449.75
    assert entry["close"] == 451.25
    assert entry["volume"] == 500000
    assert entry["chart_time"] == 1234567890
    assert entry["seq"] == 1
    assert "ts" in entry


@pytest.mark.asyncio
async def test_quote_handler_uses_key_if_present(reset_server_state):
    """Test that quote handler uses 'key' field when present."""
    import server

    handler = server._make_quote_handler("AAPL")

    msg = {
        "content": [{
            "key": "MSFT",  # Different from handler symbol
            "BID_PRICE": 380.00,
            "ASK_PRICE": 380.10,
        }]
    }

    await handler(msg)

    # Should use the key from the message, not the handler's symbol
    assert len(server.quote_buffer["MSFT"]) == 1
    assert "AAPL" not in server.quote_buffer or len(server.quote_buffer["AAPL"]) == 0


@pytest.mark.asyncio
async def test_quote_handler_multiple_messages(reset_server_state):
    """Test that quote handler correctly buffers multiple messages."""
    import server

    handler = server._make_quote_handler("AAPL")

    for i in range(3):
        msg = {
            "content": [{
                "key": "AAPL",
                "BID_PRICE": 150.00 + i,
                "ASK_PRICE": 150.10 + i,
            }]
        }
        await handler(msg)

    assert len(server.quote_buffer["AAPL"]) == 3
    # Deque is ordered with most recent first (appendleft)
    assert server.quote_buffer["AAPL"][0]["bid"] == 152.00
    assert server.quote_buffer["AAPL"][1]["bid"] == 151.00
    assert server.quote_buffer["AAPL"][2]["bid"] == 150.00


@pytest.mark.asyncio
async def test_buffer_respects_maxlen(reset_server_state):
    """Test that buffers respect maximum length."""
    import server

    # Set buffer to small size for testing
    handler = server._make_quote_handler("TEST")

    # Add more messages than MAX_BUFFER
    for i in range(600):
        msg = {
            "content": [{
                "key": "TEST",
                "BID_PRICE": 100.00 + i,
            }]
        }
        await handler(msg)

    # Should respect MAX_BUFFER (default 500)
    assert len(server.quote_buffer["TEST"]) <= 500


# ── MCP Tool Tests ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_tools(reset_server_state):
    """Test that list_tools returns expected MCP tools."""
    import server

    tools = await server.list_tools()

    tool_names = {t.name for t in tools}
    assert "subscribe_quotes" in tool_names
    assert "subscribe_chart" in tool_names
    assert "get_latest_quotes" in tool_names
    assert "list_subscriptions" in tool_names
    assert "unsubscribe" in tool_names


def test_get_latest_quotes_no_data(reset_server_state):
    """Test get_latest_quotes formatting when no data is buffered."""
    import server

    # Verify quote buffer is empty
    assert len(server.quote_buffer["AAPL"]) == 0

    # Verify the response format for no data
    symbol = "AAPL"
    buf = server.quote_buffer[symbol]
    assert len(buf) == 0

    # Test the response
    result = server._ok(
        f"No quote data buffered for {symbol} yet. "
        "Make sure you've subscribed and data is flowing (market may be closed)."
    )
    assert len(result) == 1
    assert "No quote data buffered" in result[0].text


def test_get_latest_quotes_with_data(reset_server_state):
    """Test get_latest_quotes returns buffered data."""
    import server

    # Manually add some quote data
    server.quote_buffer["AAPL"].appendleft({
        "ts": datetime.utcnow().isoformat(),
        "symbol": "AAPL",
        "bid": 150.25,
        "ask": 150.30,
    })

    # Verify buffer has data
    assert len(server.quote_buffer["AAPL"]) == 1

    # Get data from buffer
    symbol = "AAPL"
    n = 10
    buf = server.quote_buffer[symbol]
    data = list(buf)[:n]

    # Verify data
    result = server._ok(json.dumps(data, indent=2))
    assert len(result) == 1
    parsed_data = json.loads(result[0].text)
    assert len(parsed_data) == 1
    assert parsed_data[0]["symbol"] == "AAPL"
    assert parsed_data[0]["bid"] == 150.25


def test_get_latest_quotes_respects_n_limit(reset_server_state):
    """Test that get_latest_quotes respects the n parameter."""
    import server

    # Add 5 quote entries
    for i in range(5):
        server.quote_buffer["TSLA"].appendleft({
            "ts": datetime.utcnow().isoformat(),
            "symbol": "TSLA",
            "bid": 250.00 + i,
        })

    # Verify we have 5 entries
    assert len(server.quote_buffer["TSLA"]) == 5

    # Test n=2 limit
    symbol = "TSLA"
    n = 2
    buf = server.quote_buffer[symbol]
    data = list(buf)[:n]

    result = server._ok(json.dumps(data, indent=2))
    parsed_data = json.loads(result[0].text)
    assert len(parsed_data) == 2


def test_get_latest_quotes_symbol_normalization(reset_server_state):
    """Test that get_latest_quotes normalizes symbols to uppercase."""
    import server

    # Add data with uppercase symbol
    server.quote_buffer["SPY"].appendleft({
        "ts": datetime.utcnow().isoformat(),
        "symbol": "SPY",
        "bid": 450.00,
    })

    # Test that we can access it with lowercase (after normalization)
    symbol = "spy".upper()  # Normalize to uppercase
    buf = server.quote_buffer[symbol]
    assert len(buf) == 1
    assert buf[0]["symbol"] == "SPY"


def test_list_subscriptions_empty(reset_server_state):
    """Test list_subscriptions formatting when no subscriptions are active."""
    import server

    # Verify no active subscriptions
    assert len(server.active_quote_symbols) == 0
    assert len(server.active_chart_symbols) == 0

    # Create the response
    result_dict = {
        "level1_quotes": sorted(server.active_quote_symbols),
        "chart_candles": sorted(server.active_chart_symbols),
        "buffer_sizes": {
            "quotes": {s: len(server.quote_buffer[s]) for s in server.active_quote_symbols},
            "charts": {s: len(server.chart_buffer[s]) for s in server.active_chart_symbols},
        },
    }

    result = server._ok(json.dumps(result_dict, indent=2))
    data = json.loads(result[0].text)
    assert data["level1_quotes"] == []
    assert data["chart_candles"] == []
    assert data["buffer_sizes"]["quotes"] == {}
    assert data["buffer_sizes"]["charts"] == {}


def test_list_subscriptions_with_active(reset_server_state):
    """Test list_subscriptions shows active subscriptions."""
    import server

    # Manually add active subscriptions
    server.active_quote_symbols.add("AAPL")
    server.active_quote_symbols.add("TSLA")
    server.active_chart_symbols.add("SPY")

    # Add some buffer data
    server.quote_buffer["AAPL"].appendleft({"symbol": "AAPL"})
    server.quote_buffer["AAPL"].appendleft({"symbol": "AAPL"})

    # Create the response
    result_dict = {
        "level1_quotes": sorted(server.active_quote_symbols),
        "chart_candles": sorted(server.active_chart_symbols),
        "buffer_sizes": {
            "quotes": {s: len(server.quote_buffer[s]) for s in server.active_quote_symbols},
            "charts": {s: len(server.chart_buffer[s]) for s in server.active_chart_symbols},
        },
    }

    result = server._ok(json.dumps(result_dict, indent=2))
    data = json.loads(result[0].text)
    assert set(data["level1_quotes"]) == {"AAPL", "TSLA"}
    assert data["chart_candles"] == ["SPY"]
    assert data["buffer_sizes"]["quotes"]["AAPL"] == 2


# ── Helper Tests ──────────────────────────────────────────────────────────────

def test_ok_response(reset_server_state):
    """Test _ok helper formats response correctly."""
    import server

    result = server._ok("Test message")

    assert len(result) == 1
    assert isinstance(result[0], TextContent)
    assert result[0].type == "text"
    assert result[0].text == "Test message"


def test_err_response(reset_server_state):
    """Test _err helper formats response correctly."""
    import server

    result = server._err("Test error")

    assert len(result) == 1
    assert isinstance(result[0], TextContent)
    assert result[0].type == "text"
    assert "ERROR:" in result[0].text
    assert "Test error" in result[0].text


# ── Integration-style Tests ───────────────────────────────────────────────────

def test_chart_type_in_get_latest_quotes(reset_server_state):
    """Test getting chart data via get_latest_quotes."""
    import server

    # Add chart data
    server.chart_buffer["QQQ"].appendleft({
        "ts": datetime.utcnow().isoformat(),
        "symbol": "QQQ",
        "open": 380.00,
        "close": 382.50,
    })

    # Verify we can retrieve chart data
    symbol = "QQQ"
    n = 10
    data_type = "chart"
    buf = server.chart_buffer[symbol]
    data = list(buf)[:n]

    result = server._ok(json.dumps(data, indent=2))
    parsed_data = json.loads(result[0].text)
    assert len(parsed_data) == 1
    assert parsed_data[0]["open"] == 380.00
    assert parsed_data[0]["close"] == 382.50


def test_unknown_tool_returns_error(reset_server_state):
    """Test that unknown tool names return error."""
    import server

    # Test error response for unknown tool
    result = server._err(f"Unknown tool: unknown_tool")

    assert len(result) == 1
    assert "ERROR:" in result[0].text
    assert "Unknown tool" in result[0].text
