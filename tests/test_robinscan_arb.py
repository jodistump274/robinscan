import json
import unittest
from decimal import Decimal

from robinscan_arb import (
    BlockTransaction,
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
            from_address="0x637b712d51f2a7690eae7c7691d81691d290aae9",
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

    def test_multiple_swaps_without_a_closed_path_is_only_suspected(self):
        transfers, traces, pools = self.fixture()
        result = analyze_transaction(self.transaction(), transfers, [], pools)

        self.assertEqual(result.classification, "suspected")
        self.assertEqual(result.swap_count, 2)
        self.assertEqual(result.path, [])


if __name__ == "__main__":
    unittest.main()
