// Package storage 实现单机的追加日志（append-only log）。
//
// 阶段 1 的目标（对应 DDIA 第 3 章）：
//   - 记录编码 + 校验和
//   - 分段文件（segment）+ 稀疏索引
//   - 崩溃恢复：校验失败时截断半写记录
//   - 按 key 的日志压缩（compaction）
package storage
