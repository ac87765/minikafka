# 第 2 课：编码与演进

对应 DDIA 第 4 章「编码与演化」。

阶段 2 分四步，这一课是第 1 步：

| 步骤 | 内容 | 状态 |
|---|---|---|
| 2.1 | 用 Protobuf 定义记录，做一次 schema 演进，测试证明新旧版本互通 | 本课 |
| 2.2 | 磁盘格式加版本号（magic byte），新代码能读阶段 1 写的旧段 | |
| 2.3 | gRPC 服务：Produce / Fetch，一个命令行客户端 | |
| 2.4 | 混合版本测试：旧客户端连新服务端，新客户端连旧服务端 | |

## 1. 从阶段 1 的一个问题说起

阶段 1 的记录格式是：

```
| crc 4B | offset 8B | attrs 1B | keyLen 4B | valueLen 4B | key | value |
```

现在想给每条记录加一个时间戳（保留策略现在只能看段文件的 mtime，很粗糙）。
最直接的做法是在 valueLen 后面加 8 个字节。

**先停下来想一想：** 新代码去读阶段 1 写的旧段文件，会发生什么？旧代码读新代码写的文件呢？
（答案在第 6 节，先自己推一遍。）

这就是第 4 章要解决的问题：**数据比代码活得久。** 代码一部署就换了，可磁盘上的旧段、
网络上传输中的消息、还没升级的节点，都还是旧格式。

## 2. 兼容性的两个方向

| 名字 | 含义 | 什么时候需要 |
|---|---|---|
| 向后兼容 backward | 新代码能读旧数据 | 读历史数据；升级后读磁盘上的旧段 |
| 向前兼容 forward | 旧代码能读新数据 | 滚动升级时新节点先写，旧节点还要读；回滚 |

记法：站在**代码**的角度。「向后」是往回看，读过去的数据；「向前」是往前看，读未来的数据。

一个 3 节点集群做滚动升级，一次升级一台。中间有一段时间新旧并存，Leader 可能是新的，
Follower 可能是旧的，所以两个方向**都**要成立。

## 3. 为什么不用 JSON 或者 Go 自带的 gob

- **语言绑定**（gob、Java Serializable）：只有 Go 能读，而且 gob 对演进的支持很弱。
- **JSON**：可读性好，但没有二进制类型（key/value 是任意字节，要 base64，体积变大 1/3）；
  数字在 JavaScript 里超过 2^53 就失去精度（offset 是 uint64！）；字段名每条都重复一遍。
- **Protobuf / Thrift / Avro**：二进制、有 schema、为演进设计。

## 4. Protobuf 的线上格式

打开 `schema/wire_test.go`，`TestWireFormatByHand` 把一条记录的每个字节都手工核对了一遍。
核心只有一条：

```
tag = 字段编号 << 3 | wire type
```

wire type 0 是 varint（后面跟一个变长整数），2 是 LEN（后面跟长度，再跟这么多字节）。

由此可以推出几件重要的事：

1. **字节里没有字段名。** 所以改字段名不影响兼容（但会影响 JSON 编码）。
2. **字段编号是身份。** 改编号等于删了旧字段、加了新字段。
3. **不认识的字段能跳过。** 因为 tag 里有 wire type，解码器总知道这个字段多长。这就是向前兼容的来源。
4. **零值不写。** proto3 里 0、false、空字节不占空间。所以旧数据里没有新字段，读出来就是零值。
   这是向后兼容的来源，也是一个约束：新字段的零值必须有合理含义。

varint 的细节看 `TestVarintSizes`：小数字占的字节少，offset=1 只要 1 字节，比阶段 1 的定长 8 字节省；
但 uint64 最大值要 10 字节。

## 5. 这一步的演进：v1 → v2

`proto/minikafka/record.proto` 是当前 schema。v1 只有阶段 1 的四个字段，v2 加了：

- `int64 timestamp_ms = 5;` 0 表示「不知道」，也就是旧数据。
- `repeated Header headers = 6;` 应用自定义元数据。Kafka 0.11 就是这样加上 headers 的。

`proto/compat/v1/record.proto` 是 v1 的冻结快照，测试用它扮演「还没升级的旧代码」。

`schema/evolution_test.go` 证明的事：

| 测试 | 证明了什么 |
|---|---|
| `TestBackwardCompatNewReadsOld` | v2 读 v1 的数据，新字段是零值 |
| `TestForwardCompatOldReadsNew` | v1 读 v2 的数据，跳过不认识的字段，并把它们留在 unknown fields 里 |
| `TestOldCodeRewritePreservesNewFields` | v1 读、改、写回，v2 的字段还在（DDIA 图 4-7） |
| `TestOldCodeViaDomainStructLosesNewFields` | 但如果旧代码先转成自己的结构体再转回来，新字段就丢了 |
| `TestReusedFieldNumberSilentlyLosesData` | 复用字段编号，两个方向都**不报错**，数据悄悄丢了 |
| `TestWidthChangeTruncates` | int64 改成 int32，能解析，但得到一个错误的数 |

最后两个是反面教材（`proto/compat/broken/record.proto`）。它们说明了演进规则为什么是这样：

1. 编号一旦发布就永远属于那个字段，不改编号，不改类型。
2. 只加字段，新字段零值要有意义。
3. 删字段时写 `reserved 5; reserved "timestamp_ms";`，让编译器帮你挡住复用。

人会忘记规则，所以用工具检查：`buf breaking proto --against '.git#branch=main'`
会把当前 schema 和 main 分支比较，发现不兼容的改动就报错。适合放进 CI。

## 6. 第 1 节的答案（先自己想过再看）

<details>
<summary>展开</summary>

新代码读旧文件：它以为 valueLen 后面还有 8 字节时间戳，于是把 key 的前 8 个字节当成时间戳，
后面全部错位：长度字段读到的是别的字节，要么超过上限，要么 CRC 对不上，于是**几乎每一条旧记录都被当成损坏**。
更糟的是，阶段 1 的恢复逻辑会把活跃段从第一条坏记录开始截断，旧段则直接让 Open 失败。

旧代码读新文件：同样错位，同样全部判为损坏。

根本原因：阶段 1 的格式没有任何办法说「我是哪个版本」。第 2.2 步会加一个版本字节（Kafka 叫 magic byte），
这是二进制格式演进的另一种做法：不靠字段编号，而是整个格式带版本号，读者按版本选解码器。

</details>

## 7. 思考题

1. 为什么 protobuf 里删掉的字段，编号也不能再给别的字段用？结合 `TestReusedFieldNumberSilentlyLosesData`，
   说说在滚动升级的哪个时刻会出问题。
2. proto3 去掉了 `required`。假设 v1 有个 `required` 字段，v3 想删掉它，会发生什么？
   提示：想想还没升级的那台旧节点收到 v3 写的数据。
3. 我们的 Kafka 应该把 protobuf 用在哪里：网络上的 RPC 消息，还是磁盘上的每一条记录，还是都用？
   真正的 Kafka 两处都没用 protobuf，你觉得为什么？（提示：零拷贝、批量、CRC 覆盖的是哪些字节。）

## 面试官会怎么问

- 「Protobuf 怎么做到向前、向后兼容？」字段编号 + wire type 让未知字段可跳过；缺失字段取零值。
- 「Protobuf 和 Avro 的区别？」Avro 字节里连编号都没有，读的时候要拿到写者的 schema，
  和自己的 schema 做解析匹配（writer's schema vs reader's schema）。好处是更紧凑、适合动态生成 schema
  （比如从数据库表结构生成），代价是必须有地方存写者的 schema（Kafka 生态里的 Schema Registry 就是干这个的）。
- 「上线一个新字段要注意什么？」先部署能读新字段的代码（读者），再部署写新字段的代码（写者）；
  零值语义；回滚时旧代码能否容忍新数据。
