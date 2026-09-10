#!/usr/bin/env python3
"""Scan recent Robinscan blocks for closed-loop DEX arbitrage transactions."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import secrets
import socket
import ssl
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Callable, Iterable, TextIO
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

getcontext().prec = 60

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
USER_AGENT = "robinscan-arb/0.2 (+https://github.com/jodistump274/robinscan)"
NEXT_CHUNK_RE = re.compile(
    r'self\.__next_f\.push\(\s*\[\s*1\s*,\s*("(?:\\.|[^"\\])*")\s*\]\s*\)'
)
DEX_NAME_RE = re.compile(
    r"uniswap|pancake|sushi|aerodrome|curve|balancer|pool manager|\bpool\b|\bmarket\b",
    re.IGNORECASE,
)
ANCHOR_PRIORITY = (
    "WETH",
    "ETH",
    "USDG",
    "USDC",
    "USDT",
    "DAI",
    "USDS",
    "FRAX",
    "USD1",
    "PYUSD",
    "USDE",
)
SIMPLE_METHODS = {
    "system",
    "approve",
    "transfer",
    "transferfrom",
    "permit",
    "setapprovalforall",
}


class RobinscanError(RuntimeError):
    """Raised when Robinscan data cannot be fetched or decoded."""


class RobinscanStreamClosed(RobinscanError):
    """Raised when the Robinscan WebSocket closes or violates the protocol."""


def lower(value: Any) -> str:
    return str(value or "").lower()


def token_symbol(value: Any) -> str:
    return " ".join(str(value or "").split()).upper()


def decimal_value(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return default


def integer_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def decode_next_data(html: str) -> str:
    """Decode and concatenate React/Next.js flight-data string chunks."""
    chunks: list[str] = []
    for match in NEXT_CHUNK_RE.finditer(html):
        try:
            decoded = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, str):
            chunks.append(decoded)
    return "".join(chunks)


def extract_json_value(data: str, key: str, expected: type) -> Any:
    """Extract the last JSON array/object assigned to *key* from flight data."""
    opening = "[" if expected is list else "{"
    closing = "]" if expected is list else "}"
    marker = f'"{key}":{opening}'
    position = data.rfind(marker)
    if position < 0:
        return expected()

    start = data.find(opening, position + len(key) + 3)
    depth = 0
    quoted = False
    escaped = False

    for index in range(start, len(data)):
        character = data[index]
        if quoted:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                quoted = False
            continue
        if character == '"':
            quoted = True
        elif character == opening:
            depth += 1
        elif character == closing:
            depth -= 1
            if depth == 0:
                try:
                    value = json.loads(data[start : index + 1])
                except json.JSONDecodeError as exc:
                    raise RobinscanError(f"invalid JSON for {key}: {exc}") from exc
                if not isinstance(value, expected):
                    raise RobinscanError(f"{key} is not a {expected.__name__}")
                return value
    raise RobinscanError(f"unterminated JSON value for {key}")


@dataclass(slots=True)
class Block:
    number: int
    hash: str
    tx_count: int
    timestamp: str = ""


@dataclass(slots=True)
class StreamTicket:
    url: str
    ticket_protocol: str


@dataclass(slots=True)
class BlockTransaction:
    hash: str
    tx_index: int
    status: int
    from_address: str
    to_address: str
    to_is_contract: bool
    method: str
    method_id: str
    gas_used: int
    effective_gas_price: Decimal

    @property
    def gas_eth(self) -> Decimal:
        if self.gas_used <= 0 or self.effective_gas_price <= 0:
            return Decimal(0)
        return Decimal(self.gas_used) * self.effective_gas_price / Decimal(10**18)


@dataclass(slots=True)
class Transfer:
    token: str
    from_address: str
    to_address: str
    symbol: str
    amount: Decimal
    price_usd: Decimal | None
    log_index: int

    @property
    def usd(self) -> Decimal | None:
        return self.amount * self.price_usd if self.price_usd is not None else None


@dataclass(slots=True)
class Edge:
    from_symbol: str
    to_symbol: str
    dex: str
    pool: str
    fee: str = ""
    caller: str = ""


@dataclass(slots=True)
class TransactionResult:
    hash: str
    tx_index: int
    classification: str
    reason: str
    path: list[str] = field(default_factory=list)
    hops: int = 0
    swap_count: int = 0
    dexes: dict[str, int] = field(default_factory=dict)
    executor: str = ""
    profit_address: str = ""
    profit_token: str = ""
    gross_profit: Decimal | None = None
    gas_eth: Decimal | None = None
    net_profit: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("gross_profit", "gas_eth", "net_profit"):
            number = value[key]
            value[key] = format(number, "f") if number is not None else None
        return value


@dataclass(slots=True)
class BlockResult:
    block: Block
    confirmed: list[TransactionResult] = field(default_factory=list)
    suspected: list[TransactionResult] = field(default_factory=list)
    scanned_transactions: int = 0
    skipped_transactions: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "block": asdict(self.block),
            "arb_count": len(self.confirmed),
            "suspected_count": len(self.suspected),
            "scanned_transactions": self.scanned_transactions,
            "skipped_transactions": self.skipped_transactions,
            "errors": self.errors,
            "arbs": [item.to_dict() for item in self.confirmed],
            "suspected": [item.to_dict() for item in self.suspected],
        }


class RobinscanClient:
    def __init__(
        self,
        base_url: str = "https://robinscan.io",
        timeout: float = 20.0,
        retries: int = 2,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.retries = retries

    @property
    def origin(self) -> str:
        parsed = urlparse(self.base_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _get(self, path: str) -> str:
        url = urljoin(self.base_url, path.lstrip("/"))
        request = Request(
            url,
            headers={
                "Accept": "text/html,application/json",
                "User-Agent": USER_AGENT,
            },
        )
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    return response.read().decode("utf-8")
            except HTTPError as exc:
                last_error = exc
                if exc.code != 429 and exc.code < 500:
                    break
            except (URLError, TimeoutError, OSError) as exc:
                last_error = exc
            if attempt < self.retries:
                time.sleep(0.25 * (2**attempt))
        raise RobinscanError(f"GET {url} failed: {last_error}")

    def stream_ticket(self, client_id: str | None = None) -> StreamTicket:
        """Request the short-lived credentials used by Robinscan's live stream."""
        client_id = client_id or secrets.token_hex(16)
        if not re.fullmatch(r"[0-9a-f]{32}", client_id):
            raise ValueError("client_id must be 16 bytes encoded as 32 lowercase hex characters")

        query = urlencode({"clientId": client_id})
        payload = json.loads(self._get(f"/api/stream-ticket?{query}"))
        if not isinstance(payload, dict):
            raise RobinscanError("stream-ticket response is not a JSON object")
        if isinstance(payload.get("data"), dict):
            payload = payload["data"]

        stream_url = next(
            (
                str(payload[key])
                for key in ("url", "wsUrl", "websocketUrl")
                if isinstance(payload.get(key), str) and payload[key]
            ),
            "",
        )
        ticket_protocol = next(
            (
                str(payload[key])
                for key in ("ticketProtocol", "ticket", "token")
                if isinstance(payload.get(key), str) and payload[key]
            ),
            "",
        )
        parsed = urlparse(stream_url)
        fields = ", ".join(sorted(payload))
        if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
            raise RobinscanError(
                f"stream-ticket response has no valid WebSocket URL (fields: {fields})"
            )
        if not ticket_protocol or not re.fullmatch(
            r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+", ticket_protocol
        ):
            raise RobinscanError(
                f"stream-ticket response has no valid ticketProtocol (fields: {fields})"
            )
        return StreamTicket(stream_url, ticket_protocol)

    def blocks(self, limit: int) -> list[Block]:
        limit = max(1, min(limit, 100))
        payload = json.loads(self._get(f"/api/blocks?page=1&pageSize={limit}"))
        return [
            Block(
                number=integer_value(item.get("number")),
                hash=str(item.get("hash") or ""),
                tx_count=integer_value(item.get("txCount")),
                timestamp=str(item.get("timestamp") or ""),
            )
            for item in payload.get("items", [])
        ]

    def block_transactions(self, number: int) -> list[BlockTransaction]:
        html = self._get(f"/block/{number}")
        data = decode_next_data(html)
        raw = extract_json_value(data, "transactions", list)
        transactions: list[BlockTransaction] = []
        for item in raw:
            transactions.append(
                BlockTransaction(
                    hash=str(item.get("hash") or ""),
                    tx_index=integer_value(item.get("txIndex")),
                    status=integer_value(item.get("status")),
                    from_address=lower(item.get("from")),
                    to_address=lower(item.get("to")),
                    to_is_contract=bool(item.get("toIsContract")),
                    method=str(item.get("method") or ""),
                    method_id=lower(item.get("methodId")),
                    gas_used=integer_value(item.get("gasUsed")),
                    effective_gas_price=decimal_value(
                        item.get("effectiveGasPrice") or item.get("gasPrice"), Decimal(0)
                    )
                    or Decimal(0),
                )
            )
        return transactions

    def transaction_payload(self, tx_hash: str) -> tuple[list[Any], list[Any], dict[str, Any]]:
        html = self._get(f"/tx/{tx_hash}")
        data = decode_next_data(html)
        transfers = extract_json_value(data, "tokenTransfers", list)
        traces = extract_json_value(data, "internalTransactions", list)
        pools = extract_json_value(data, "pools", dict)
        return transfers, traces, pools


class RobinscanWebSocket:
    """Dependency-free WebSocket client for Robinscan update notifications.

    Robinscan data frames use Borsh. A complete data frame is deliberately used
    only as a wake-up signal; authoritative block and transaction details are
    then fetched over HTTP and handled by the existing detector.
    """

    PROTOCOL = "robinscan.borsh.v2"
    MAX_HEADER_BYTES = 64 * 1024
    MAX_FRAME_BYTES = 16 * 1024 * 1024

    def __init__(self, connection: socket.socket, buffered: bytes = b"") -> None:
        self.connection = connection
        self.buffered = bytearray(buffered)
        self.fragment_opcode: int | None = None
        self.closed = False

    @classmethod
    def connect(
        cls,
        ticket: StreamTicket,
        origin: str,
        timeout: float = 20.0,
    ) -> "RobinscanWebSocket":
        parsed = urlparse(ticket.url)
        if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
            raise RobinscanError(f"invalid stream URL: {ticket.url}")

        secure = parsed.scheme == "wss"
        port = parsed.port or (443 if secure else 80)
        host = parsed.hostname
        host_header = f"[{host}]" if ":" in host else host
        if port != (443 if secure else 80):
            host_header = f"{host_header}:{port}"
        path = parsed.path or "/"
        if parsed.query:
            path += f"?{parsed.query}"

        raw_connection: socket.socket | None = None
        connection: socket.socket | None = None
        try:
            raw_connection = socket.create_connection((host, port), timeout=timeout)
            if secure:
                context = ssl.create_default_context()
                connection = context.wrap_socket(raw_connection, server_hostname=host)
            else:
                connection = raw_connection
            connection.settimeout(timeout)

            websocket_key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
            request = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host_header}\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Origin: {origin.rstrip('/')}\r\n"
                f"Sec-WebSocket-Key: {websocket_key}\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                f"Sec-WebSocket-Protocol: {cls.PROTOCOL}, {ticket.ticket_protocol}\r\n"
                f"User-Agent: {USER_AGENT}\r\n"
                "\r\n"
            ).encode("ascii")
            connection.sendall(request)

            header, remainder = cls._read_handshake(connection)
            cls._validate_handshake(header, websocket_key)
            return cls(connection, remainder)
        except Exception:
            if connection is not None:
                connection.close()
            elif raw_connection is not None:
                raw_connection.close()
            raise

    @classmethod
    def _read_handshake(cls, connection: socket.socket) -> tuple[bytes, bytes]:
        data = bytearray()
        marker = b"\r\n\r\n"
        while marker not in data:
            chunk = connection.recv(4096)
            if not chunk:
                raise RobinscanStreamClosed("stream closed during WebSocket handshake")
            data.extend(chunk)
            if len(data) > cls.MAX_HEADER_BYTES:
                raise RobinscanStreamClosed("WebSocket response headers are too large")
        end = data.index(marker) + len(marker)
        return bytes(data[:end]), bytes(data[end:])

    @classmethod
    def _validate_handshake(cls, raw_header: bytes, websocket_key: str) -> None:
        try:
            lines = raw_header.decode("iso-8859-1").split("\r\n")
            status = int(lines[0].split(" ", 2)[1])
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if ":" not in line:
                    continue
                name, value = line.split(":", 1)
                headers[name.strip().lower()] = value.strip()
        except (IndexError, ValueError) as exc:
            raise RobinscanStreamClosed("invalid WebSocket handshake response") from exc

        if status != 101:
            raise RobinscanStreamClosed(f"WebSocket handshake returned HTTP {status}")
        expected_accept = base64.b64encode(
            hashlib.sha1(
                (websocket_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
            ).digest()
        ).decode("ascii")
        if headers.get("sec-websocket-accept") != expected_accept:
            raise RobinscanStreamClosed("WebSocket handshake has an invalid accept key")
        if headers.get("sec-websocket-protocol") != cls.PROTOCOL:
            raise RobinscanStreamClosed(
                "WebSocket server did not select the robinscan.borsh.v2 protocol"
            )

    def _read_exactly(self, length: int) -> bytes:
        data = bytearray()
        if self.buffered:
            take = min(length, len(self.buffered))
            data.extend(self.buffered[:take])
            del self.buffered[:take]
        while len(data) < length:
            chunk = self.connection.recv(length - len(data))
            if not chunk:
                raise RobinscanStreamClosed("Robinscan live stream disconnected")
            data.extend(chunk)
        return bytes(data)

    def _read_frame(self) -> tuple[bool, int, bytes]:
        first, second = self._read_exactly(2)
        final = bool(first & 0x80)
        if first & 0x70:
            raise RobinscanStreamClosed("unsupported WebSocket RSV bits")
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exactly(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exactly(8))[0]
        if length > self.MAX_FRAME_BYTES:
            raise RobinscanStreamClosed(f"WebSocket frame exceeds {self.MAX_FRAME_BYTES} bytes")
        mask = self._read_exactly(4) if masked else b""
        payload = self._read_exactly(length)
        if masked:
            payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        return final, opcode, payload

    def _send_frame(self, opcode: int, payload: bytes = b"") -> None:
        if self.closed:
            return
        if len(payload) > 125:
            raise ValueError("control frame payload is too large")
        mask = secrets.token_bytes(4)
        header = bytes((0x80 | opcode, 0x80 | len(payload))) + mask
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        self.connection.sendall(header + masked)

    def wait_for_update(self, timeout: float) -> bool:
        """Wait for one complete data message; return False on an idle timeout."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self.connection.settimeout(remaining)
            try:
                final, opcode, payload = self._read_frame()
            except socket.timeout:
                return False

            if opcode == 0x8:
                if not self.closed:
                    self._send_frame(0x8, payload[:125])
                self.closed = True
                self.connection.close()
                code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1000
                raise RobinscanStreamClosed(f"Robinscan live stream closed ({code})")
            if opcode == 0x9:
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode in {0x1, 0x2}:
                if self.fragment_opcode is not None:
                    raise RobinscanStreamClosed(
                        "new message started before fragmented message ended"
                    )
                if final:
                    return True
                self.fragment_opcode = opcode
                continue
            if opcode == 0x0:
                if self.fragment_opcode is None:
                    raise RobinscanStreamClosed("unexpected WebSocket continuation frame")
                if final:
                    self.fragment_opcode = None
                    return True
                continue
            raise RobinscanStreamClosed(f"unsupported WebSocket opcode {opcode}")

    def close(self) -> None:
        if self.closed:
            self.connection.close()
            return
        try:
            self._send_frame(0x8, struct.pack("!H", 1000))
        except OSError:
            pass
        finally:
            self.closed = True
            self.connection.close()

    def __enter__(self) -> "RobinscanWebSocket":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def parse_transfers(raw: list[dict[str, Any]]) -> tuple[list[Transfer], dict[str, str]]:
    transfers: list[Transfer] = []
    token_map: dict[str, str] = {}
    for item in raw:
        meta = item.get("tokenMeta") or {}
        symbol = token_symbol(meta.get("symbol") or "TOKEN")
        decimals = integer_value(meta.get("decimals"), 18)
        raw_amount = decimal_value(item.get("value"), Decimal(0)) or Decimal(0)
        amount = raw_amount / (Decimal(10) ** decimals)
        price = decimal_value(meta.get("priceUsd"))
        token = lower(item.get("token"))
        token_map[token] = symbol
        transfers.append(
            Transfer(
                token=token,
                from_address=lower(item.get("from")),
                to_address=lower(item.get("to")),
                symbol=symbol,
                amount=amount,
                price_usd=price,
                log_index=integer_value(item.get("logIndex")),
            )
        )
    transfers.sort(key=lambda transfer: transfer.log_index)
    return transfers, token_map


def parameter_value(decoded: dict[str, Any], name: str) -> Any:
    for parameter in decoded.get("parameters") or []:
        if parameter.get("name") == name:
            return parameter.get("value")
    return None


def build_edges(
    transfers: list[Transfer], traces: list[dict[str, Any]], pools: dict[str, Any], token_map: dict[str, str]
) -> tuple[list[Edge], set[str], set[str]]:
    for pool in pools.values():
        base = pool.get("base") or {}
        quote = pool.get("quote") or {}
        if base and quote:
            token_map[lower(base.get("address"))] = token_symbol(base.get("symbol"))
            token_map[lower(quote.get("address"))] = token_symbol(quote.get("symbol"))

    wrapped = "WETH" if "WETH" in token_map.values() else "ETH"

    def symbol_of(address: Any) -> str:
        normalized = lower(address)
        if normalized == ZERO_ADDRESS:
            return wrapped
        return token_map.get(normalized) or normalized[:10]

    v4_edges: list[Edge] = []
    for trace in traces:
        decoded = trace.get("decodedInput") or {}
        method_id = lower(decoded.get("methodId") or str(trace.get("input") or "")[:10])
        if trace.get("success") is not True or method_id != "0xf3cd914c":
            continue
        key = parameter_value(decoded, "key")
        params = parameter_value(decoded, "params")
        if not isinstance(key, list) or len(key) < 5 or not isinstance(params, list) or not params:
            continue
        zero_for_one = params[0] is True or str(params[0]).lower() == "true"
        token0, token1 = symbol_of(key[0]), symbol_of(key[1])
        v4_edges.append(
            Edge(
                from_symbol=token0 if zero_for_one else token1,
                to_symbol=token1 if zero_for_one else token0,
                dex="Uniswap V4",
                pool=lower(trace.get("to")),
                fee=str(key[2]),
                caller=lower(trace.get("from")),
            )
        )

    v3_addresses = {
        lower(address)
        for address, pool in pools.items()
        if "v3" in lower(pool.get("venue"))
    }
    dex_addresses = v3_addresses | {edge.pool for edge in v4_edges}
    v3_edges: list[Edge] = []

    for address, pool in pools.items():
        pool_address = lower(address)
        venue = lower(pool.get("venue"))
        if "v3" not in venue:
            continue
        paid = {
            transfer.symbol
            for transfer in transfers
            if transfer.to_address == pool_address and transfer.from_address not in dex_addresses
        }
        received = {
            transfer.symbol
            for transfer in transfers
            if transfer.from_address == pool_address and transfer.to_address not in dex_addresses
        }
        if len(paid) == 1 and len(received) == 1:
            v3_edges.append(
                Edge(
                    from_symbol=next(iter(paid)),
                    to_symbol=next(iter(received)),
                    dex="Pancake V3" if "pancake" in venue else "Uniswap V3",
                    pool=pool_address,
                )
            )

    edges = v3_edges + v4_edges
    for edge in edges:
        if edge.from_symbol == "ETH":
            edge.from_symbol = wrapped
        if edge.to_symbol == "ETH":
            edge.to_symbol = wrapped
    return edges, v3_addresses, dex_addresses


def find_cycle(edges: list[Edge], preferred_start: str = "") -> tuple[list[str], list[Edge]]:
    best_tokens: list[str] = []
    best_edges: list[Edge] = []
    starts = [preferred_start] if preferred_start else []
    starts.extend(edge.from_symbol for edge in edges if edge.from_symbol not in starts)
    limit = min(len(edges), 10)

    for start in starts:
        if not start:
            continue

        def walk(current: str, used: set[int], tokens: list[str], route: list[Edge]) -> None:
            nonlocal best_tokens, best_edges
            if route and current == start:
                if len(route) > len(best_edges):
                    best_tokens = tokens.copy()
                    best_edges = route.copy()
                return
            if len(route) >= limit:
                return
            for index, edge in enumerate(edges):
                if index in used or edge.from_symbol != current:
                    continue
                used.add(index)
                walk(edge.to_symbol, used, tokens + [edge.to_symbol], route + [edge])
                used.remove(index)

        walk(start, set(), [start], [])
    return best_tokens, best_edges


def find_longest_path(
    edges: list[Edge], preferred_start: str = ""
) -> tuple[list[str], list[Edge]]:
    """Return the longest directed, edge-simple path, even when it is open."""
    best_tokens: list[str] = []
    best_edges: list[Edge] = []
    starts = [preferred_start] if preferred_start else []
    starts.extend(edge.from_symbol for edge in edges if edge.from_symbol not in starts)
    limit = min(len(edges), 10)

    for start in starts:
        if not start:
            continue

        def walk(current: str, used: set[int], tokens: list[str], route: list[Edge]) -> None:
            nonlocal best_tokens, best_edges
            if len(route) > len(best_edges):
                best_tokens = tokens.copy()
                best_edges = route.copy()
            if len(route) >= limit:
                return
            for index, edge in enumerate(edges):
                if index in used or edge.from_symbol != current:
                    continue
                used.add(index)
                walk(edge.to_symbol, used, tokens + [edge.to_symbol], route + [edge])
                used.remove(index)

        walk(start, set(), [start], [])
    return best_tokens, best_edges


def select_executor(
    transfers: list[Transfer], traces: list[dict[str, Any]], edges: list[Edge], v3_addresses: set[str], dex_addresses: set[str]
) -> str:
    callers: dict[str, int] = {}
    for edge in edges:
        if edge.caller:
            callers[edge.caller] = callers.get(edge.caller, 0) + 1
    if callers:
        return max(callers, key=callers.get)

    token_addresses = {transfer.token for transfer in transfers}
    callbacks = [
        lower(trace.get("to"))
        for trace in traces
        if trace.get("type") == "call"
        and lower(trace.get("from")) in v3_addresses
        and lower(trace.get("to")) not in token_addresses
    ]
    if callbacks:
        return max(set(callbacks), key=callbacks.count)

    scores: dict[str, int] = {}
    for transfer in transfers:
        for address in (transfer.from_address, transfer.to_address):
            if address != ZERO_ADDRESS and address not in dex_addresses:
                scores[address] = scores.get(address, 0) + 1
    return max(scores, key=scores.get) if scores else ""


def estimate_profit(
    executor: str,
    transaction_from: str,
    transaction_to: str,
    profit_token: str,
    transfers: list[Transfer],
    traces: list[dict[str, Any]],
    dex_addresses: set[str],
) -> tuple[str, str, Decimal | None, Decimal | None]:
    """Measure the closed route's token surplus across the searcher's actor group.

    Transfers inside the group are ignored. Capital supplied by the transaction
    sender, flash liquidity, repayments and pool flows therefore contribute only
    their net change instead of being mistaken for terminal profit.
    """
    if not profit_token:
        return "", "", None, None

    flash_providers: set[str] = set()
    all_dex = set(dex_addresses)
    for trace in traces:
        decoded = trace.get("decodedInput") or {}
        method_id = lower(decoded.get("methodId") or str(trace.get("input") or "")[:10])
        if re.search(r"flashLoan\s*\(", str(decoded.get("methodCall") or ""), re.I) or method_id == "0xe0232b42":
            flash_providers.add(lower(trace.get("to")))
        if DEX_NAME_RE.search(f"{trace.get('fromName', '')} {trace.get('toName', '')}"):
            all_dex.add(lower(trace.get("from")))
            all_dex.add(lower(trace.get("to")))

    actors = {
        address
        for address in (executor, lower(transaction_from), lower(transaction_to))
        if address and address != ZERO_ADDRESS
    }
    token_addresses = {transfer.token for transfer in transfers}
    changed = True
    while changed:
        changed = False
        for transfer in transfers:
            if (
                transfer.from_address in actors
                and transfer.to_address != ZERO_ADDRESS
                and transfer.to_address not in all_dex
                and transfer.to_address not in flash_providers
                and transfer.to_address not in token_addresses
                and transfer.to_address not in actors
            ):
                actors.add(transfer.to_address)
                changed = True

    balances: dict[str, Decimal] = {}
    prices: dict[str, Decimal] = {}
    for transfer in transfers:
        balance_symbol = transfer.symbol
        if profit_token in {"ETH", "WETH"} and transfer.symbol in {"ETH", "WETH"}:
            balance_symbol = profit_token
        if transfer.price_usd is not None:
            prices[balance_symbol] = transfer.price_usd
        from_actor = transfer.from_address in actors
        to_actor = transfer.to_address in actors
        if to_actor and not from_actor:
            balances[balance_symbol] = balances.get(balance_symbol, Decimal(0)) + transfer.amount
        elif from_actor and not to_actor:
            balances[balance_symbol] = balances.get(balance_symbol, Decimal(0)) - transfer.amount

    gross = balances.get(profit_token)
    profit_address = executor if gross is not None else ""
    if gross is not None and gross > 0:
        for transfer in transfers:
            if (
                (
                    transfer.symbol == profit_token
                    or {
                        transfer.symbol,
                        profit_token,
                    }
                    == {"ETH", "WETH"}
                )
                and transfer.from_address in actors
                and transfer.to_address in actors
                and transfer.to_address != executor
            ):
                later_out = any(
                    other.log_index > transfer.log_index
                    and other.from_address == transfer.to_address
                    and other.token == transfer.token
                    for other in transfers
                )
                if not later_out:
                    profit_address = transfer.to_address

    gross_usd: Decimal | None = None
    if gross is not None and profit_token in prices:
        gross_usd = gross * prices[profit_token]
    return profit_token, profit_address, gross, gross_usd


def analyze_transaction(
    transaction: BlockTransaction,
    raw_transfers: list[dict[str, Any]],
    traces: list[dict[str, Any]],
    pools: dict[str, Any],
) -> TransactionResult:
    transfers, token_map = parse_transfers(raw_transfers)
    edges, v3_addresses, dex_addresses = build_edges(transfers, traces, pools, token_map)
    executor = select_executor(transfers, traces, edges, v3_addresses, dex_addresses)
    edge_symbols = {
        symbol
        for edge in edges
        for symbol in (edge.from_symbol, edge.to_symbol)
    }
    preferred = next(
        (symbol for symbol in ANCHOR_PRIORITY if symbol in edge_symbols),
        "",
    )
    path, route = find_cycle(edges, preferred)
    closed = len(route) >= 2 and bool(path) and path[0] == path[-1]
    if not closed:
        path, route = find_longest_path(edges, preferred)

    profit_token = path[0] if closed else ""
    profit_address = ""
    gross: Decimal | None = None
    gross_usd: Decimal | None = None
    if closed:
        profit_token, profit_address, gross, gross_usd = estimate_profit(
            executor,
            transaction.from_address,
            transaction.to_address,
            profit_token,
            transfers,
            traces,
            dex_addresses,
        )

    dexes: dict[str, int] = {}
    for edge in route:
        dexes[edge.dex] = dexes.get(edge.dex, 0) + 1

    gas_eth = transaction.gas_eth if transaction.gas_eth > 0 else None
    net_profit: Decimal | None = None
    if gross is not None and profit_token in {"ETH", "WETH"} and gas_eth is not None:
        net_profit = gross - gas_eth
    elif gross_usd is not None:
        net_profit = None

    positive = gross is not None and gross > 0
    if closed and positive:
        classification = "confirmed"
        reason = "closed swap path with positive net actor-group token surplus"
    elif closed and gross is None:
        classification = "suspected"
        reason = "closed swap path, but the route-token balance could not be measured"
    elif closed:
        classification = "suspected"
        reason = "closed swap path, but no positive net route-token surplus was measured"
    elif len(route) >= 2:
        classification = "suspected"
        reason = "connected swaps found, but the directed path is open"
    else:
        classification = "none"
        reason = "fewer than two directionally connected swaps"

    return TransactionResult(
        hash=transaction.hash,
        tx_index=transaction.tx_index,
        classification=classification,
        reason=reason,
        path=path,
        hops=len(route),
        swap_count=len(edges),
        dexes=dexes,
        executor=executor,
        profit_address=profit_address,
        profit_token=profit_token,
        gross_profit=gross,
        gas_eth=gas_eth,
        net_profit=net_profit,
    )


def should_scan(transaction: BlockTransaction) -> bool:
    method = transaction.method.strip().lower().replace(" ", "")
    return (
        bool(transaction.hash)
        and transaction.status == 1
        and transaction.tx_index != 0
        and transaction.to_is_contract
        and method not in SIMPLE_METHODS
    )


class Scanner:
    def __init__(self, client: RobinscanClient, concurrency: int = 4) -> None:
        self.client = client
        self.concurrency = max(1, concurrency)

    def scan_block(self, block: Block) -> BlockResult:
        result = BlockResult(block=block)
        transactions = self.client.block_transactions(block.number)
        if block.tx_count == 0:
            block.tx_count = len(transactions)
        candidates = [transaction for transaction in transactions if should_scan(transaction)]
        result.skipped_transactions = len(transactions) - len(candidates)

        def scan_one(transaction: BlockTransaction) -> TransactionResult:
            transfers, traces, pools = self.client.transaction_payload(transaction.hash)
            return analyze_transaction(transaction, transfers, traces, pools)

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = {executor.submit(scan_one, transaction): transaction for transaction in candidates}
            for future in as_completed(futures):
                transaction = futures[future]
                result.scanned_transactions += 1
                try:
                    item = future.result()
                except Exception as exc:  # keep scanning the rest of the block
                    result.errors.append(f"{transaction.hash}: {exc}")
                    continue
                if item.classification == "confirmed":
                    result.confirmed.append(item)
                elif item.classification == "suspected":
                    result.suspected.append(item)

        result.confirmed.sort(key=lambda item: item.tx_index)
        result.suspected.sort(key=lambda item: item.tx_index)
        return result


class LiveWatcher:
    """Track the indexed tip and emit each newly confirmed ARB exactly once."""

    def __init__(
        self,
        client: RobinscanClient,
        scanner: Scanner,
        *,
        json_output: bool = False,
        details: bool = False,
        index_retries: int = 2,
        retry_delay: float = 0.75,
        output: TextIO | None = None,
        error_output: TextIO | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client = client
        self.scanner = scanner
        self.json_output = json_output
        self.details = details
        self.index_retries = max(0, index_retries)
        self.retry_delay = max(0.0, retry_delay)
        self.output = output or sys.stdout
        self.error_output = error_output or sys.stderr
        self.sleep = sleep
        self.cursor = 0
        self.tip_hash = ""
        self.emitted_transactions: set[str] = set()

    def initialize(self) -> Block:
        blocks = self.client.blocks(1)
        if not blocks:
            raise RobinscanError("Robinscan returned no latest block")
        tip = max(blocks, key=lambda block: block.number)
        self.cursor = tip.number
        self.tip_hash = tip.hash
        return tip

    def _scan_with_retry(self, block: Block) -> BlockResult:
        result: BlockResult | None = None
        for attempt in range(self.index_retries + 1):
            result = self.scanner.scan_block(block)
            observed = result.scanned_transactions + result.skipped_transactions
            incomplete = bool(result.errors) or (block.tx_count > 0 and observed == 0)
            if not incomplete or attempt == self.index_retries:
                return result
            self.sleep(self.retry_delay * (attempt + 1))
        assert result is not None
        return result

    def _emit(self, result: BlockResult) -> None:
        for item in result.confirmed:
            if item.hash in self.emitted_transactions:
                continue
            self.emitted_transactions.add(item.hash)
            if self.json_output:
                event = {
                    "event": "arb",
                    "block": asdict(result.block),
                    "arb": item.to_dict(),
                }
                print(
                    json.dumps(event, ensure_ascii=False, separators=(",", ":")),
                    file=self.output,
                    flush=True,
                )
                continue

            path = " -> ".join(item.path) if item.path else "unresolved"
            profit = ""
            if item.gross_profit is not None:
                profit = (
                    f" gross={format_decimal(item.gross_profit)} {item.profit_token}"
                )
            if item.net_profit is not None:
                profit += f" net={format_decimal(item.net_profit)} {item.profit_token}"
            extra = ""
            if self.details:
                dexes = ",".join(
                    f"{name}x{count}" for name, count in sorted(item.dexes.items())
                ) or "-"
                extra = (
                    f" dex={dexes} executor={item.executor or '-'}"
                    f" profit_to={item.profit_address or '-'}"
                )
            print(
                f"  ARB {item.hash} block={result.block.number} "
                f"tx_index={item.tx_index} {path}{profit}{extra}",
                file=self.output,
                flush=True,
            )

        if result.errors:
            print(
                f"[live] block {result.block.number}: {len(result.errors)} transaction "
                "error(s); an ARB may have been missed",
                file=self.error_output,
                flush=True,
            )
            for error in result.errors:
                print(f"[live] ERR {error}", file=self.error_output, flush=True)

    def scan_available(self) -> int:
        """Scan every block after the cursor, oldest first; return blocks scanned."""
        recent = self.client.blocks(100)
        if not recent:
            raise RobinscanError("Robinscan returned no blocks while live")
        by_number = {block.number: block for block in recent if block.number > 0}
        tip = max(by_number.values(), key=lambda block: block.number)

        if self.cursor == 0:
            self.cursor = tip.number
            self.tip_hash = tip.hash
            return 0
        if tip.number < self.cursor:
            return 0

        if tip.number == self.cursor:
            if not tip.hash or not self.tip_hash or tip.hash == self.tip_hash:
                return 0
            pending = [tip]
            print(
                f"[live] block {tip.number} hash changed; rescanning replacement block",
                file=self.error_output,
                flush=True,
            )
        else:
            pending = [
                by_number.get(number, Block(number=number, hash="", tx_count=0))
                for number in range(self.cursor + 1, tip.number + 1)
            ]

        for block in pending:
            result = self._scan_with_retry(block)
            self._emit(result)
            self.cursor = block.number
            if block.hash:
                self.tip_hash = block.hash
        if pending[-1].number == tip.number:
            self.tip_hash = tip.hash
        return len(pending)


def run_live(
    client: RobinscanClient,
    scanner: Scanner,
    *,
    interval: float,
    json_output: bool,
    details: bool,
    index_retries: int,
) -> None:
    watcher = LiveWatcher(
        client,
        scanner,
        json_output=json_output,
        details=details,
        index_retries=index_retries,
    )
    tip = watcher.initialize()
    print(
        f"[live] baseline block {tip.number}; waiting for newly indexed blocks",
        file=sys.stderr,
        flush=True,
    )
    print(
        "[live] stdout is intentionally silent until a confirmed closed-loop ARB appears",
        file=sys.stderr,
        flush=True,
    )

    reconnect_delay = 1.0
    while True:
        try:
            ticket = client.stream_ticket()
            with RobinscanWebSocket.connect(ticket, client.origin, client.timeout) as stream:
                print(
                    "[live] Robinscan WebSocket connected",
                    file=sys.stderr,
                    flush=True,
                )
                reconnect_delay = 1.0
                last_poll = 0.0
                while True:
                    signaled = stream.wait_for_update(interval)
                    now = time.monotonic()
                    if signaled and now - last_poll < 0.5:
                        time.sleep(0.5 - (now - last_poll))
                    watcher.scan_available()
                    last_poll = time.monotonic()
        except (RobinscanError, HTTPError, URLError, json.JSONDecodeError, OSError) as exc:
            print(
                f"[live] stream unavailable ({exc}); polling and retrying in "
                f"{format_decimal(Decimal(str(reconnect_delay)), 1)}s",
                file=sys.stderr,
                flush=True,
            )
            retry_at = time.monotonic() + reconnect_delay
            while True:
                remaining = retry_at - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(interval, remaining))
                try:
                    watcher.scan_available()
                except (RobinscanError, HTTPError, URLError, json.JSONDecodeError, OSError) as poll_error:
                    print(f"[live] polling error: {poll_error}", file=sys.stderr, flush=True)
            reconnect_delay = min(max(interval, reconnect_delay * 2), 30.0)


def format_decimal(value: Decimal | None, places: int = 9) -> str:
    if value is None:
        return "-"
    text = f"{value:.{places}f}".rstrip("0").rstrip(".")
    return text or "0"


def print_human(result: BlockResult, details: bool) -> None:
    print(
        f"block {result.block.number:<10} tx={result.block.tx_count:<3} "
        f"ARB={len(result.confirmed):<2} suspected={len(result.suspected):<2} "
        f"scanned={result.scanned_transactions:<3} skipped={result.skipped_transactions:<3} "
        f"errors={len(result.errors)}"
    )
    if not details:
        return
    for marker, items in (("ARB", result.confirmed), ("?", result.suspected)):
        for item in items:
            path = " -> ".join(item.path) if item.path else "unresolved"
            if item.path and item.path[0] != item.path[-1]:
                path += " [open]"
            net = (
                f" net={format_decimal(item.net_profit)} {item.profit_token}"
                if item.net_profit is not None
                else ""
            )
            profit = ""
            if item.gross_profit is not None:
                profit = (
                    f"  gross={format_decimal(item.gross_profit)} "
                    f"{item.profit_token}{net}"
                )
            print(f"  {marker:<3} {item.hash}  {path}{profit}")
    for error in result.errors:
        print(f"  ERR {error}", file=sys.stderr)


def scan_blocks(scanner: Scanner, blocks: Iterable[Block], json_output: bool, details: bool) -> None:
    results = [scanner.scan_block(block) for block in blocks]
    if json_output:
        print(json.dumps([result.to_dict() for result in results], ensure_ascii=False, indent=2))
    else:
        for result in results:
            print_human(result, details)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Count confirmed and suspected closed-loop DEX arbitrage transactions per Robinscan block."
    )
    parser.add_argument("--block", type=int, action="append", help="scan one block; may be repeated")
    parser.add_argument("--limit", type=int, default=25, help="number of recent blocks (default: 25, max: 100)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--watch", action="store_true", help="continue polling and print every block")
    mode.add_argument(
        "--live",
        action="store_true",
        help="subscribe to new blocks and print confirmed ARBs only",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="watch interval or live-mode polling fallback in seconds",
    )
    parser.add_argument(
        "--live-index-retries",
        type=int,
        default=2,
        help="retries for a newly indexed block with incomplete data (default: 2)",
    )
    parser.add_argument("--concurrency", type=int, default=4, help="concurrent transaction requests per block")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=2, help="HTTP retries for 429/5xx/network errors")
    parser.add_argument("--base-url", default="https://robinscan.io", help="Robinscan base URL")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--details", action="store_true", help="show matching transaction details")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.interval < 0.5:
        raise SystemExit("--interval must be at least 0.5 seconds")
    if args.live_index_retries < 0:
        raise SystemExit("--live-index-retries cannot be negative")
    if args.block and (args.watch or args.live):
        parser.error("--block cannot be combined with --watch or --live")
    client = RobinscanClient(args.base_url, args.timeout, args.retries)
    scanner = Scanner(client, args.concurrency)

    try:
        if args.block:
            blocks = [Block(number=number, hash="", tx_count=0) for number in args.block]
            scan_blocks(scanner, blocks, args.json, args.details)
            return 0

        if args.live:
            run_live(
                client,
                scanner,
                interval=args.interval,
                json_output=args.json,
                details=args.details,
                index_retries=args.live_index_retries,
            )
            return 0

        seen: set[str] = set()
        while True:
            blocks = client.blocks(args.limit)
            pending = [block for block in blocks if block.hash not in seen]
            if args.watch:
                pending.reverse()
            scan_blocks(scanner, pending, args.json, args.details)
            seen.update(block.hash for block in pending)
            if not args.watch:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 130
    except (RobinscanError, HTTPError, URLError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
