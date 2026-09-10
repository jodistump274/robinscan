import base64
import hashlib
import io
import json
import re
import unittest
from decimal import Decimal
from unittest.mock import patch

from robinscan_arb import (
    Block,
    BlockResult,
    BlockTransaction,
    LiveWatcher,
    WebSocketConnection,
    TransactionResult,
    analyze_transaction,
    decode_next_data,
    extract_json_value,
)


class NextDataTests(unittest.TestCase):
    def test_decodes_split_flight_chunks_and_extracts_last_value(self):
        first = '{"translations":{"transactions":"Transactions"},'
        second = '"transactions":[{"hash":"0xabc","status":1}]}'
        html = (
            '<script>self.__next_f.push([1,' + json.dumps(first) + '])</script>'
            '<script>self.__next_f.push([1,' + json.dumps(second) + '])</script>'
        )
        data = decode_next_data(html)
        transactions = extract_json_value(data, "transactions", list)
        self.assertEqual(transactions, [{"hash": "0xabc", "status": 1}])


class StreamTests(unittest.TestCase):
    def test_sequencer_feed_handshake_and_binary_update_without_dependency(self):
        class HandshakeSocket:
            def __init__(self):
                self.incoming = bytearray()
                self.sent = []
                self.timeout = None
                self.closed = False

            def settimeout(self, timeout):
                self.timeout = timeout

            def sendall(self, data):
                self.sent.append(data)
                if not data.startswith(b"GET "):
                    return
                request = data.decode("ascii")
                key = re.search(r"Sec-WebSocket-Key: ([^\r]+)", request).group(1)
                accept = base64.b64encode(
                    hashlib.sha1(
                        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
                    ).digest()
                ).decode("ascii")
                response = (
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Accept: {accept}\r\n"
                    "\r\n"
                ).encode("ascii")
                self.incoming.extend(response + b"\x82\x01\x01")

            def recv(self, length):
                if not self.incoming:
                    return b""
                data = bytes(self.incoming[:length])
                del self.incoming[:length]
                return data

            def close(self):
                self.closed = True

        connection = HandshakeSocket()
        with patch("robinscan_arb.socket.create_connection", return_value=connection):
            stream = WebSocketConnection.connect(
                "ws://feed.example/api/stream?source=test",
                timeout=1,
            )

        request = connection.sent[0].decode("ascii")
        self.assertIn("GET /api/stream?source=test HTTP/1.1", request)
        self.assertNotIn("Sec-WebSocket-Protocol", request)
        self.assertNotIn("Origin:", request)
        self.assertTrue(stream.wait_for_update(1))
        stream.close()
        self.assertTrue(connection.closed)


class LiveWatcherTests(unittest.TestCase):
    def test_skips_baseline_and_emits_only_new_confirmed_arbs_once(self):
        baseline = Block(100, "0x100", 1)
        recent = [Block(102, "0x102", 1), Block(101, "0x101", 1), baseline]

        class Client:
            calls = 0

            def blocks(self, _limit):
                self.calls += 1
                return [baseline] if self.calls == 1 else recent

        class FakeScanner:
            def __init__(self):
                self.blocks = []

            def scan_block(self, block):
                self.blocks.append(block.number)
                result = BlockResult(block=block, scanned_transactions=1)
                if block.number == 101:
                    result.suspected.append(
                        TransactionResult(
                            hash="0xsuspect",
                            tx_index=2,
                            classification="suspected",
                            reason="open",
                            path=["WETH", "TOKEN"],
                        )
                    )
                if block.number == 102:
                    result.confirmed.append(
                        TransactionResult(
                            hash="0xarb",
                            tx_index=3,
                            classification="confirmed",
                            reason="closed",
                            path=["WETH", "TOKEN", "USDG", "WETH"],
                            profit_token="WETH",
                            gross_profit=Decimal("0.01"),
                            net_profit=Decimal("0.009"),
                        )
                    )
                return result

        output = io.StringIO()
        errors = io.StringIO()
        scanner = FakeScanner()
        watcher = LiveWatcher(
            Client(),
            scanner,
            output=output,
            error_output=errors,
            retry_delay=0,
        )

        self.assertEqual(watcher.initialize().number, 100)
        self.assertEqual(scanner.blocks, [])
        self.assertEqual(watcher.scan_available(), 2)
        self.assertEqual(scanner.blocks, [101, 102])
        self.assertEqual(watcher.scan_available(), 0)

        lines = output.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("  ARB 0xarb "))
        self.assertIn("block=102", lines[0])
        self.assertIn("WETH -> TOKEN -> USDG -> WETH", lines[0])
        self.assertNotIn("suspect", output.getvalue())
        self.assertEqual(errors.getvalue(), "")


class DetectorTests(unittest.TestCase):
    ZERO = "0x0000000000000000000000000000000000000000"
    PAR = "0x507b6f349a80114097a67b8b4677367acc15b220"
    WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
    USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
    MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
    PAR_POOL = "0x5cddcc8e02bf1fac92440cf0287f8eff4ef7e4fc"
    WETH_POOL = "0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca"
    EXECUTOR = "0x980ee195eed44af65e8345f5e06fff1a6d8a9f89"
    RECEIVER = "0x1c5d8792098bcd216a593f4f87618088c6f5dde0"
    CALLER = "0x637b712d51f2a7690eae7c7691d81691d290aae9"

    def transfer(self, token, from_address, to_address, amount, index):
        token_name = "par" if token == self.PAR else "WETH" if token == self.WETH else "USDG"
        decimals = 6 if token_name == "USDG" else 18
        value = Decimal(amount) * (Decimal(10) ** decimals)
        return {
            "token": token,
            "from": from_address,
            "to": to_address,
            "logIndex": index,
            "value": str(int(value)),
            "tokenMeta": {"symbol": token_name, "decimals": decimals},
        }

    def fixture(self):
        transfers = [
            self.transfer(self.PAR, self.MANAGER, self.EXECUTOR, "455127.049610", 0),
            self.transfer(self.USDG, self.PAR_POOL, self.EXECUTOR, "1495.176250", 1),
            self.transfer(self.PAR, self.EXECUTOR, self.PAR_POOL, "455127.049610", 2),
            self.transfer(self.WETH, self.WETH_POOL, self.EXECUTOR, "0.601900992", 3),
            self.transfer(self.USDG, self.EXECUTOR, self.WETH_POOL, "1495.176250", 4),
            self.transfer(self.WETH, self.EXECUTOR, self.ZERO, "0.588023000", 5),
            self.transfer(self.WETH, self.EXECUTOR, self.RECEIVER, "0.013877992", 6),
        ]
        pools = {
            self.PAR_POOL: {
                "venue": "uniswap-v3",
                "base": {"address": self.PAR, "symbol": "par"},
                "quote": {"address": self.USDG, "symbol": "USDG"},
            },
            self.WETH_POOL: {
                "venue": "uniswap-v3",
                "base": {"address": self.WETH, "symbol": "WETH"},
                "quote": {"address": self.USDG, "symbol": "USDG"},
            },
        }
        traces = [
            {
                "type": "call",
                "success": True,
                "from": self.EXECUTOR,
                "to": self.MANAGER,
                "index": 0,
                "decodedInput": {
                    "methodId": "0xf3cd914c",
                    "parameters": [
                        {"name": "key", "value": [self.ZERO, self.PAR, 10000, 200, self.ZERO]},
                        {"name": "params", "value": [True, "-588023000000000000", "0"]},
                    ],
                },
            },
            {
                "type": "call",
                "success": True,
                "from": self.PAR_POOL,
                "to": self.EXECUTOR,
                "index": 1,
                "input": "0xfa461e33",
            },
        ]
        return transfers, traces, pools

    def transaction(self):
        return BlockTransaction(
            hash="0x8522794ada37abc62ef1f834525b4e6e9385dc3beef3644c502e342bfc719e7e",
            tx_index=1,
            status=1,
            from_address=self.CALLER,
            to_address=self.EXECUTOR,
            to_is_contract=True,
            method="execute",
            method_id="0x12345678",
            gas_used=379533,
            effective_gas_price=Decimal("372721213"),
        )

    def test_detects_case_insensitive_triangular_arbitrage_and_net_profit(self):
        transfers, traces, pools = self.fixture()
        result = analyze_transaction(self.transaction(), transfers, traces, pools)

        self.assertEqual(result.classification, "confirmed")
        self.assertEqual(result.path, ["WETH", "PAR", "USDG", "WETH"])
        self.assertEqual(result.hops, 3)
        self.assertEqual(result.swap_count, 3)
        self.assertEqual(result.profit_address, self.RECEIVER)
        self.assertEqual(result.gross_profit, Decimal("0.013877992"))
        self.assertEqual(result.net_profit.quantize(Decimal("0.000000001")), Decimal("0.013736532"))

    def test_supplied_capital_is_not_counted_as_profit(self):
        transfers, traces, pools = self.fixture()
        transfers.append(
            self.transfer(self.WETH, self.CALLER, self.EXECUTOR, "0.588023000", -1)
        )
        result = analyze_transaction(self.transaction(), transfers, traces, pools)

        self.assertEqual(result.classification, "confirmed")
        self.assertEqual(result.gross_profit, Decimal("0.013877992"))
        self.assertEqual(result.profit_address, self.RECEIVER)

    def test_multiple_swaps_without_a_closed_path_is_only_suspected(self):
        transfers, traces, pools = self.fixture()
        result = analyze_transaction(self.transaction(), transfers, [], pools)

        self.assertEqual(result.classification, "suspected")
        self.assertEqual(result.swap_count, 2)
        self.assertEqual(result.path, ["PAR", "USDG", "WETH"])
        self.assertEqual(result.hops, 2)
        self.assertIsNone(result.gross_profit)
        self.assertEqual(result.profit_token, "")
        self.assertEqual(result.profit_address, "")


if __name__ == "__main__":
    unittest.main()
