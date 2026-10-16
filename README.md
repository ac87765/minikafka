# minikafka

用 Go 从零实现一个分区、多副本的追加日志（迷你 Kafka），用来学习 DDIA（《数据密集型应用系统设计》）。

## 快速开始

```bash
git clone https://github.com/ac87765/minikafka.git
cd minikafka
go run ./cmd/demo     # 演示：写入、分段、读取、压缩、断电恢复
go test ./...         # 跑全部测试
```

第一次用 Go 和 VS Code？看 [docs/SETUP.md](docs/SETUP.md)。

## 路线图

| 阶段 | 内容 | DDIA | 状态 |
|---|---|---|---|
| 1 | 单机存储：segment、稀疏索引、崩溃恢复、压缩 | 第 3 章 | ✅ |
| 2 | Protobuf 记录与 RPC、schema 演进 | 第 4 章 | |
| 3 | Leader-Follower 复制、ISR、HW、acks | 第 5 章 | |
| 4 | 分区、哈希路由、再平衡 | 第 6 章 | |
| 5 | Raft 管理元数据 | 第 8、9 章 | |
| 6 | 消费者组、offset、投递语义、物化视图 | 第 7、11、12 章 | |

## 阶段 1 做了什么、保证了什么

记录格式：

```
| crc 4B | offset 8B | attrs 1B | keyLen 4B | valueLen 4B | key | value |
```

| 保证 | 怎么做到的 | 对应测试 |
|---|---|---|
| 任何一个比特出错都能发现 | CRC-32C 覆盖 crc 之后的全部字节 | `TestBitFlipAnywhereIsDetected` |
| 写到一半断电，重启后数据一致 | 只检查最后一段，从第一条坏记录截断并 fsync | `TestRecoverTornWriteAtEveryByte` |
| 旧段损坏不会被悄悄丢弃 | 非活跃段出现坏记录时 Open 直接报错 | `TestCorruptionInOldSegmentFailsOpen` |
| 按 offset 读取不用扫全文件 | 段文件名 = 起始 offset（二分）+ 段内稀疏索引 | `TestSegmentAppendRead` |
| 删除旧数据只需删文件 | 按大小和时间滚动分段，只删最老的连续前缀 | `TestLogRetention` |
| 压缩不改变最终状态 | 每个 key 只留最新值，墓碑保留一段时间，offset 不重新编号 | `TestCompactionPreservesState` |
| 压缩中途崩溃不丢数据 | 写临时文件 → fsync → rename | `TestCrashDuringCompaction` |

**不保证的：** 单机上，活跃段中间出现位翻转时，截断会丢掉其后已经确认过的记录。这要靠阶段 3 的副本解决。

课程笔记在 [lessons/](lessons/)。
