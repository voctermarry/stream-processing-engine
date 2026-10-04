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
| `describe` | 打印能力清单（聚合、窗口规格、事件字段、退出码、检查点格式版本） | 0 |
| `windows --window <spec> --from <ms> --to <ms>` | 打印与区间相交的窗口边界 | 0 / 2 |
| `run --input <jsonl> [--window] [--aggregation] [--allowed-lateness] [--max-out-of-orderness] [--output] [--checkpoint <path> \| --resume <path>]` | 处理事件并按行输出结果；可持久化检查点并断点恢复 | 0 / 2 |
| `replay --input <jsonl> [--compare <jsonl>] …` | 处理两遍并逐字段对账；给出 `identical`，带 `--compare` 时再给出 `matchesReference` | 0 / 2 / **3** |

`--input -` 读 stdin；未给 `--output` 写 stdout。**退出码 3** 表示"报告已产出但对账不一致"——报告本身仍然完整可读。

## 检查点与断点恢复（仅 `run`）

产品级的持久化检查点让一次 `run` 可以中途失败、随后从断点继续，且**最终结果与一次不中断的 run 逐字节一致**。

- 首次执行：`run … --checkpoint <path> --output <结果文件>`。每成功解析并处理一行输入，就用"临时文件 + 原子替换"重写一次检查点。
- 恢复执行：`run … --resume <path> --output <结果文件>`。读取同一文件、校验后从下一行继续，并继续原子更新它。
- `--checkpoint` 与 `--resume` **互斥**。启用任一选项时，`--input` 必须是普通文件（不能是 stdin），且必须显式给出非 stdout 的 `--output`；stdin/stdout 组合一律返回 `validation_error`。
- 恢复时沿用检查点保存的窗口、聚合、迟到与乱序配置；**显式传入冲突配置会返回 `validation_error`，绝不静默覆盖**（不传则沿用，传相同值允许）。
- 成功结束时：先按既有方式原子替换完整结果文件，再留下 `status="completed"` 的完成态检查点；从完成态再次 `--resume` 幂等地重写同一份结果。

检查点是带固定格式标识与版本号的**规范化单行 JSON**，完整保存：已消费行数 `consumed`、已消费前缀的 sha256 `prefixDigest`、键控窗口/会话状态、水位线与迟到计数、已发射记录身份 `state.emitted` 以及尚未提交到最终文件的规范化输出 `emitted`。恢复会先逐字节校验已消费前缀未变化（并拒绝越界偏移），前缀事件不重复计入，已缓存的窗口结果不重复、不遗漏。

```bash
stream-processing-engine run --input events.jsonl --window tumbling:100 \
    --checkpoint state.ckpt --output results.jsonl
# …若在某行后失败（或后续行 parse_error），检查点停在最后一个成功行，results.jsonl 尚未生成…
stream-processing-engine run --input events.jsonl --window tumbling:100 \
    --resume state.ckpt --output results.jsonl
```

检查点相关失败的错误分类：

| 情况 | 错误类型 |
|---|---|
| 无法读取检查点、无法原子写入检查点/结果 | `output_error` |
| 检查点不是合法 JSON（带 `line`/`column`） | `parse_error` |
| 格式标识未知、版本不支持、字段缺失/类型非法、多出未知字段 | `validation_error` |
| 保存配置与恢复传入配置冲突、输入前缀校验失败、偏移超出当前输入 | `validation_error` |

任何恢复校验失败都**不会改动**既有结果文件或检查点；处理后续输入时发生解析错误，结果文件保持原样，检查点停在最后一个成功行。不带新选项时，`run` 的输出、错误与退出码与旧版完全一致。

## 保障

- 未指定 `--output` 时只写 stdout；指定后**先写临时文件再原子替换**，失败不会留下半成品，也不会破坏旧文件。
- 检查点逐行原子替换；恢复先校验已消费前缀（sha256）再继续，任何校验失败都不触碰既有产物。
- 输出路径与任何输入路径相同 ⇒ 在读取之前报 `output_error`。
- `windows` 只支持 `tumbling` 规格；其它规格报 `validation_error`。
- 所有错误文档形如 `{"error":"<kind>","message":"…", …}`，`kind` 稳定可取。

## 目录

```
stream_processing/errors.py    异常层次（kind + 上下文）
stream_processing/events.py    事件解析与水位线（含状态快照）
stream_processing/windows.py   滚动/滑动/会话窗口、会话合并与窗口规格解析
stream_processing/pipeline.py  有状态聚合、发射与状态快照/恢复
stream_processing/checkpoint.py 持久化检查点、前缀校验与断点恢复执行
stream_processing/cli.py       四个子命令与退出码
tests/                         窗口数学、解析错误、CLI 契约、恢复确定性、检查点契约
```
