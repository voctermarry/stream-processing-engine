# stream-processing-engine

有状态事件时间流处理引擎（Python 标准库实现，无第三方依赖）。

## 安装与入口

```
python3 -m pip install -e .
stream-processing-engine --help
stream-processing-engine describe
```

包名 `stream_processing`，命令行入口 `stream-processing-engine`。所有命令**只在 stdout 输出 JSON**（一行一个文档），错误只写 stderr 并以退出码结束。

## 数据模型

事件是 JSON Lines，每行一个对象：

```json
{"timestamp": 1700000000000, "key": "sensor-a", "value": 3.5, "kind": "data"}
```

| 字段 | 必需 | 说明 |
|---|---|---|
| `timestamp` | 是 | 整数毫秒（Unix epoch）；**不接受**浮点或字符串 |
| `key` | 是 | 非空字符串，聚合按 key 分开 |
| `value` | 否 | 数字，缺省 `0`；`kind=punct` 时忽略 |
| `kind` | 否 | `data`（默认，计入聚合）或 `punct`（推进时间的标点，不计值） |

未知字段、类型错误、非法 JSON 都以 `parse_error` 报出，并带 `line`（1 起）与 `column`。

## 窗口

| 说明 | 规格 |
|---|---|
| 滚动窗口（不重叠，按 `offset` 对齐） | `tumbling:<size>[:<offset>]` |
| 滑动窗口（`slide < size` 时重叠） | `sliding:<size>:<slide>[:<offset>]` |
| 会话窗口（间隔 ≤ `gap` 合并） | `session:<gap>` |

窗口是**半开区间** `[start, end)`；`end - start = size`。

## 水位线与迟到

水位线 = `max_seen - max_out_of_orderness`。窗口在**水位线到达其 `end`**（再加 `--allowed-lateness`）时发射。

**输出与输入行序无关——前提是乱序落在 `--max-out-of-orderness` 覆盖范围内。** 超出该范围的事件在到达时水位线已过其窗口，会被判为迟到（这是有意的语义，不是缺陷）：例如 `10,20,30,110` 按 `tumbling:100`、`out-of-orderness=0` 正序处理得两个窗口，逆序处理则因 110 先到而使 `10/20/30` 全部迟到，只剩一个窗口。要消除这种顺序依赖，就把 `--max-out-of-orderness` 设到覆盖真实乱序。

低于当前水位线的事件是**迟到事件**：计入 `late_dropped` 后丢弃，**绝不**写回已经发射的窗口。时间推进也可用 `kind=punct` 显式表达。

## 聚合

`count` · `sum` · `min` · `max` · `mean`。结果为：

```json
{"aggregation":"sum","count":3,"key":"sensor-a","value":10.5,"window":{"end":1700000001000,"start":1700000000000}}
```

**规范化输出**：键排序、分隔符无空格、`ensure_ascii=false`、每行以 `\n` 结束。字段顺序与数值格式是契约的一部分。

结果按 `(window.start, key)` 升序。

## 子命令

| 命令 | 作用 | 退出码 |
|---|---|---|
| `describe` | 打印能力清单（聚合、窗口规格、事件字段、退出码） | 0 |
| `windows --window <spec> --from <ms> --to <ms>` | 打印与区间相交的窗口边界 | 0 / 2 |
| `run --input <jsonl> [--window] [--aggregation] [--allowed-lateness] [--max-out-of-orderness] [--output]` | 处理事件并按行输出结果 | 0 / 2 |
| `replay --input <jsonl> [--compare <jsonl>] …` | 处理两遍并逐字段对账；给出 `identical`，带 `--compare` 时再给出 `matchesReference` | 0 / 2 / **3** |

`--input -` 读 stdin；未给 `--output` 写 stdout。**退出码 3** 表示"报告已产出但对账不一致"——报告本身仍然完整可读。

## 保障

- 未指定 `--output` 时只写 stdout；指定后**先写临时文件再原子替换**，失败不会留下半成品，也不会破坏旧文件。
- 输出路径与任何输入路径相同 ⇒ 在读取之前报 `output_error`。
- `windows` 只支持 `tumbling` 规格；其它规格报 `validation_error`。
- 所有错误文档形如 `{"error":"<kind>","message":"…", …}`，`kind` 稳定可取。

## 目录

```
stream_processing/errors.py    异常层次（kind + 上下文）
stream_processing/events.py    事件解析与水位线
stream_processing/windows.py   滚动/滑动/会话窗口与会话合并
stream_processing/pipeline.py  有状态聚合与发射
stream_processing/cli.py       四个子命令与退出码
tests/                         窗口数学、解析错误、CLI 契约
```
