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
| `value` | 否 | 数字，缺省 `0`；必须是**有限数值**——`NaN`/`Infinity`/`-Infinity` 及 `1e400` 这类溢写字面量一律 `parse_error`；`kind=punct` 时忽略（但仍校验） |
| `kind` | 否 | `data`（默认，计入聚合）或 `punct`（推进时间的标点，不计值） |

未知字段、类型错误、非法 JSON 都以 `parse_error` 报出，并带 `line`（1 起）与 `column`；非标准数值常量的 `column` 定位到该常量的首字符。直接构造 `Event` 交给 `Pipeline.add` 时，非有限 `value` 以 `ValidationError` 拒绝，且水位线、计数器、窗口值与已发射集合均不变。

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
| `describe` | 打印能力清单（聚合、窗口规格、事件字段、退出码、检查点格式版本与恢复支持） | 0 |
| `windows --window <spec> --from <ms> --to <ms>` | 打印与区间相交的窗口边界 | 0 / 2 |
| `run --input <jsonl> [--window] [--aggregation] [--allowed-lateness] [--max-out-of-orderness] [--output]` | 处理事件并按行输出结果 | 0 / 2 |
| `run … --checkpoint <cp>` / `--resume <cp>` | 持久化检查点与断点续跑（见下） | 0 / 2 |
| `replay --input <jsonl> [--compare <jsonl>] …` | 处理两遍并逐字段对账；给出 `identical`，带 `--compare` 时再给出 `matchesReference` | 0 / 2 / **3** |

`--input -` 读 stdin；未给 `--output` 写 stdout。**退出码 3** 表示"报告已产出但对账不一致"——报告本身仍然完整可读。

## 检查点与断点恢复

`run` 支持可持久化检查点，使一次中断的执行能从最后一个成功处理的输入行继续，且**最终结果文件与相同配置下一次不中断的 run 逐字节一致**。

- 首次执行：`run --input <文件> --output <文件> --checkpoint <cp>`。
- 恢复执行：`run --input <文件> --output <文件> --resume <cp>`，读取并**继续更新同一个文件**。
- `--checkpoint` 与 `--resume` 互斥；启用任一项时，`--input` 必须是普通文件（不能是 stdin），`--output` 必须显式给出文件（不能是 stdout），否则统一返回 `validation_error`。
- `--checkpoint` 的目标文件必须不存在（避免误覆盖）；`--resume` 的目标必须存在。

**逐行原子落盘。** 每成功解析并处理一行，检查点文件就以"临时文件 + 原子替换"重写一次。处理后续输入发生 `parse_error` 时：输出文件保持原样，检查点停留在最后一个成功行（`status:"running"`）；修好输入后用 `--resume` 续跑即可。

**恢复严格校验（任何一项失败都不会改动既有输出或检查点）：**

- 先用 SHA-256 校验输入中**已消费前缀逐字节未变**，再从下一行继续；既不重计前缀事件，也不重复/遗漏已缓存的窗口结果；
- 窗口、聚合、迟到与乱序配置沿用检查点保存的值；调用方显式传入冲突项（哪怕只是一项）返回 `validation_error`，**绝不静默覆盖**；不传则继承（包括默认值）；
- 已消费偏移超出当前输入行数，返回 `validation_error`。

**完成态。** 成功结束时先按原有方式原子替换完整结果文件，再留下 `status:"complete"` 的检查点。对完成态再次 `--resume` 是幂等的：重写同一份结果并刷新同一完成态标记；若完成后输入又增长（行数变多），返回 `validation_error`。

**检查点格式**是规范化的单行 JSON（排序键、无空格、末尾一个换行），带固定标识与版本号，完整保存：

| 字段 | 内容 |
|---|---|
| `format` / `version` / `status` | 固定标识 `stream-processing-checkpoint`、格式版本（当前 `1`）、`running` / `complete` |
| `config` | 保存的 `window` / `aggregation` / `allowedLateness` / `maxOutOfOrderness` |
| `consumed` | 已成功处理的输入行数 |
| `prefixSha256` | 已消费前缀原始字节（含换行符）的 SHA-256 |
| `state.values` | 键控窗口/会话状态：`[start,end,key,[values…]]` |
| `state.emitted` | 已发射记录身份：`[start,end,key]` |
| `state.watermarkMaxSeen` / `observed` / `lateDropped` | 水位线位置与迟到计数 |
| `pending` | 已产生但尚未提交到最终文件的规范化输出行（有序） |

检查点相关错误一律复用稳定错误种类：无法读取或原子写入检查点 ⇒ `output_error`；不是合法 JSON ⇒ 带 `line`/`column` 的 `parse_error`；格式标识未知、版本不支持、字段或类型非法、保存配置冲突、输入前缀校验失败、偏移超出当前输入 ⇒ `validation_error`。`describe` 在保留原字段的基础上以 `checkpoint.format` / `checkpoint.version` / `checkpoint.resume` 公开格式版本与恢复支持。

## 保障

- 数值域是有限的双精度浮点：输入只接受有限数值，输出只产生有限数值。即使每个输入 `value` 都有限，`sum`/`mean` 的结果仍可能溢出（如 `1e308 + 1e308`）——此时在产生该窗口结果之前返回 `validation_error`：stdout 不出现部分结果，既有 `--output` 文件保持原样，检查点执行不提交最终输出且检查点停留在最后一条完整成功处理的输入行（绝不保存含非有限值的状态），`replay` 不产出对账报告而以退出码 2 失败。
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
stream_processing/checkpoint.py 检查点格式、严格校验、原子写与状态绑定
stream_processing/cli.py       四个子命令与退出码
tests/                         窗口数学、解析错误、CLI 契约、恢复确定性与检查点
```
