# Robinscan ARB Scanner

扫描 Robinhood Chain 的 Robinscan 区块，统计每个区块中的确认套利交易和疑似套利交易。

项目直接读取 Robinscan 已公开的数据：

1. `/api/blocks` 获取最新区块；
2. `/block/{number}` 获取区块交易；
3. `/tx/{hash}` 获取 Token Transfers、Internal Transactions 和 Pools；
4. 根据实际资产流向重建 DEX Swap Path。

不需要 RPC、API Key 或第三方 Python 包。

## 判定标准

本工具按“套利交易笔数”计数，不按 Swap/Hop 数计数。一笔三角套利仍算一笔 ARB。

### Confirmed ARB

同时满足：

- 交易执行成功；
- 至少两个可识别的 Swap；
- Swap 方向形成资产闭环，例如 `WETH -> PAR -> USDG -> WETH`；
- 能在执行器或最终利润接收地址识别到正数的终端利润。

### Suspected

以下情况会单独列为疑似，不计入确认 ARB：

- Swap 已闭环，但 Robinscan 数据不足以计算终端利润；
- 检测到多个 Swap，但部分池或资产无法解析，Path 没有闭环。

`ETH` 与 `WETH` 在闭环判断时视为同一种基础资产。

当前识别重点是 Robinscan 已标注的 Uniswap/Pancake V3 池，以及成功执行的 Uniswap V4 `PoolManager.swap`。后续可以继续增加 V2、Curve、Balancer 等专用解析器。

## 快速运行

需要 Python 3.11 或更高版本。

```bash
git clone https://github.com/jodistump274/robinscan.git
cd robinscan
python robinscan_arb.py --limit 25 --details
```

示例输出：

```text
block 57587427   tx=8   ARB=0  suspected=0  scanned=5  skipped=3  errors=0
block 57587426   tx=14  ARB=2  suspected=1  scanned=9  skipped=5  errors=0
  ARB 0x...  WETH -> PAR -> USDG -> WETH  gross=0.013877992 WETH net=0.013736532 WETH
  ?   0x...  incomplete  gross=unknown
```

## 常用命令

扫描最近 25 个区块：

```bash
python robinscan_arb.py --limit 25
```

显示匹配交易的 Path、毛利润和净利：

```bash
python robinscan_arb.py --limit 25 --details
```

扫描指定区块：

```bash
python robinscan_arb.py --block 57587427 --details
```

持续监听新块：

```bash
python robinscan_arb.py --watch --limit 10 --interval 2 --details
```

输出 JSON，方便保存或接入前端：

```bash
python robinscan_arb.py --limit 25 --json > blocks.json
```

降低或提高交易请求并发：

```bash
python robinscan_arb.py --limit 25 --concurrency 3
```

完整参数：

```bash
python robinscan_arb.py --help
```

## JSON 字段

每个区块会输出：

- `arb_count`：确认套利交易数；
- `suspected_count`：疑似交易数；
- `scanned_transactions`：实际分析的候选交易数；
- `skipped_transactions`：失败、System、普通授权/转账等跳过数量；
- `errors`：抓取或解析失败的交易；
- `arbs`：确认套利的交易哈希、Path、DEX、毛利润、Gas 和净利；
- `suspected`：疑似交易及未确认原因。

如果 `errors` 不为空，不应把该区块的结果理解成绝对的 `ARB=0`。

## 关于 Backrun

闭环 Path 只能确认套利行为，不能单独证明 Backrun。

要确认 Backrun，还需要比较同块中排在套利交易之前的交易，验证：

1. 两笔交易使用至少一个相同池；
2. 前一笔交易先改变池价；
3. 套利交易随后沿相反或闭环方向吃掉价差。

因此当前版本统计通用 ARB，但不会把每一笔 ARB 自动标成 Backrun。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试包含之前分析的三角套利结构：

```text
WETH -> PAR -> USDG -> WETH
```

并验证：

- `par`/`PAR` 大小写不会破坏闭环；
- 三个 Swap 被识别为一笔三角套利；
- 毛利润 `0.013877992 WETH`；
- 扣除 Gas 后净利约 `0.013736532 WETH`；
- 缺少 V4 Hop 时只标记为疑似。

## 注意

Robinscan 并没有直接提供 `arb_count` 字段。本工具依赖其页面中的 Next.js Flight 数据结构。如果 Robinscan 改版导致字段变化，解析器可能需要同步更新。
