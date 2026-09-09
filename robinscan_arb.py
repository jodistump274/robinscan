#!/usr/bin/env python3
"""Scan recent Robinscan blocks for closed-loop DEX arbitrage transactions."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

getcontext().prec = 60

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
NEXT_CHUNK_RE = re.compile(
    r'self\.__next_f\.push\(\s*\[\s*1\s*,\s*("(?:\\.|[^"\\])*")\s*\]\s*\)'
)
DEX_NAME_RE = re.compile(
    r"uniswap|pancake|sushi|aerodrome|curve|balancer|pool manager|\bpool\b|\bmarket\b",
    re.IGNORECASE,
)
ANCHORS = {
    "ETH",
    "WETH",
    "USDG",
    "USDC",
    "USDT",
    "DAI",
    "USDS",
    "FRAX",
    "USD1",
    "PYUSD",
    "USDE",
}
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

    def _get(self, path: str) -> str:
        url = urljoin(self.base_url, path.lstrip("/"))
        request = Request(
            url,
            headers={
                "Accept": "text/html,application/json",
                "User-Agent": "robinscan-arb/0.1 (+https://github.com/jodistump274/robinscan)",
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
    transfers: list[Transfer],
    traces: list[dict[str, Any]],
    dex_addresses: set[str],
) -> tuple[str, str, Decimal | None, Decimal | None]:
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

    actors = {executor} if executor else set()
    changed = True
    while changed:
        changed = False
        for transfer in transfers:
            if (
                transfer.from_address in actors
                and transfer.to_address != ZERO_ADDRESS
                and transfer.to_address not in all_dex
                and transfer.to_address not in flash_providers
                and transfer.to_address not in actors
            ):
                actors.add(transfer.to_address)
                changed = True

    sinks: list[Transfer] = []
    for transfer in transfers:
        if (
            transfer.from_address not in actors
            or transfer.to_address == ZERO_ADDRESS
            or transfer.to_address in all_dex
            or transfer.to_address in flash_providers
        ):
            continue
        later_out = any(
            other.log_index > transfer.log_index
            and other.from_address == transfer.to_address
            and other.token == transfer.token
            for other in transfers
        )
        if not later_out:
            sinks.append(transfer)

    direct = sinks[-1] if sinks else None
    balances: dict[str, Decimal] = {}
    prices: dict[str, Decimal] = {}
    for transfer in transfers:
        if transfer.price_usd is not None:
            prices[transfer.symbol] = transfer.price_usd
        if transfer.to_address == executor:
            balances[transfer.symbol] = balances.get(transfer.symbol, Decimal(0)) + transfer.amount
        if transfer.from_address == executor:
            balances[transfer.symbol] = balances.get(transfer.symbol, Decimal(0)) - transfer.amount

    retained = sorted(
        ((name, amount) for name, amount in balances.items() if amount > Decimal("1e-18")),
        key=lambda item: (prices.get(item[0], Decimal(1)) * item[1]),
        reverse=True,
    )
    profit_token = direct.symbol if direct else (retained[0][0] if retained else "")
    retained_same = balances.get(profit_token, Decimal(0))
    if retained_same < 0:
        retained_same = Decimal(0)
    gross = (direct.amount + retained_same) if direct else (retained[0][1] if retained else None)
    profit_address = direct.to_address if direct else (executor if gross is not None else "")
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
    profit_token, profit_address, gross, gross_usd = estimate_profit(
        executor, transfers, traces, dex_addresses
    )
    wrapped = "WETH" if "WETH" in token_map.values() else "ETH"
    preferred = wrapped if profit_token == "ETH" else profit_token
    path, route = find_cycle(edges, preferred)

    dexes: dict[str, int] = {}
    for edge in route:
        dexes[edge.dex] = dexes.get(edge.dex, 0) + 1

    gas_eth = transaction.gas_eth if transaction.gas_eth > 0 else None
    net_profit: Decimal | None = None
    if gross is not None and profit_token in {"ETH", "WETH"} and gas_eth is not None:
        net_profit = gross - gas_eth
    elif gross_usd is not None:
        net_profit = None

    closed = len(route) >= 2 and bool(path) and path[0] == path[-1]
    positive = gross is not None and gross > 0
    if closed and positive:
        classification = "confirmed"
        reason = "closed swap path with measurable positive terminal profit"
    elif closed:
        classification = "suspected"
        reason = "closed swap path, but terminal profit could not be measured"
    elif len(edges) >= 2:
        classification = "suspected"
        reason = "multiple swaps found, but the directed path is incomplete"
    else:
        classification = "none"
        reason = "fewer than two connected swaps"

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
            path = " -> ".join(item.path) if item.path else "incomplete"
            profit = (
                f"{format_decimal(item.gross_profit)} {item.profit_token}"
                if item.gross_profit is not None
                else "unknown"
            )
            net = (
                f" net={format_decimal(item.net_profit)} {item.profit_token}"
                if item.net_profit is not None
                else ""
            )
            print(f"  {marker:<3} {item.hash}  {path}  gross={profit}{net}")
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
    parser.add_argument("--watch", action="store_true", help="continue polling for new blocks")
    parser.add_argument("--interval", type=float, default=2.0, help="watch polling interval in seconds")
    parser.add_argument("--concurrency", type=int, default=4, help="concurrent transaction requests per block")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=2, help="HTTP retries for 429/5xx/network errors")
    parser.add_argument("--base-url", default="https://robinscan.io", help="Robinscan base URL")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--details", action="store_true", help="show matching transaction details")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.interval < 0.5:
        raise SystemExit("--interval must be at least 0.5 seconds")
    client = RobinscanClient(args.base_url, args.timeout, args.retries)
    scanner = Scanner(client, args.concurrency)

    try:
        if args.block:
            blocks = [Block(number=number, hash="", tx_count=0) for number in args.block]
            scan_blocks(scanner, blocks, args.json, args.details)
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
