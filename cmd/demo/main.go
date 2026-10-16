// demo 把阶段 1 的存储引擎从头到尾跑一遍，每一步打印磁盘上发生了什么。
//
// 运行：go run ./cmd/demo
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/ac87765/minikafka/storage"
)

func main() {
	dir, err := os.MkdirTemp("", "minikafka-demo-")
	check(err)
	defer os.RemoveAll(dir)
	fmt.Println("数据目录：", dir)

	// 段很小（每段约 100 字节），方便看到分段效果
	cfg := storage.Config{MaxSegmentBytes: 100, IndexInterval: 32}
	l, err := storage.Open(dir, cfg)
	check(err)

	step("1. 写入记录（同一个 key 写多次，再删除一个）")
	writes := []struct{ k, v string }{
		{"alice", "上海"}, {"bob", "北京"}, {"alice", "杭州"},
		{"carol", "深圳"}, {"bob", "成都"}, {"alice", "广州"},
	}
	for _, w := range writes {
		off, err := l.Append([]byte(w.k), []byte(w.v))
		check(err)
		fmt.Printf("  offset %d: %s = %s\n", off, w.k, w.v)
	}
	off, err := l.Delete([]byte("carol"))
	check(err)
	fmt.Printf("  offset %d: 删除 carol（墓碑）\n", off)
	off, err = l.Append([]byte("dave"), []byte("西安"))
	check(err)
	fmt.Printf("  offset %d: dave = 西安\n", off)
	showFiles(dir)

	step("2. 按 offset 读取")
	for _, o := range []uint64{0, 4} {
		r, err := l.Read(o)
		check(err)
		fmt.Printf("  Read(%d) -> %s = %s\n", o, r.Key, r.Value)
	}

	step("3. 重放日志，得到每个 key 的最新值（物化视图）")
	showState(l)

	step("4. 压缩：每个 key 只留最新值（活跃段不动，墓碑暂时保留）")
	stats, err := l.Compact()
	check(err)
	fmt.Printf("  重写了 %d 个段，删除了 %d 条过时记录\n", stats.SegmentsRewritten, stats.RecordsRemoved)
	showFiles(dir)
	fmt.Println("  压缩后重放，结果应该和第 3 步完全一样：")
	showState(l)
	check(l.Close())

	step("5. 模拟断电：最后一个段的尾部只写了一半")
	last := lastLogFile(dir)
	data, err := os.ReadFile(last)
	check(err)
	check(os.WriteFile(last, append(data, 0xde, 0xad, 0xbe), 0o644)) // 追加 3 个垃圾字节
	fmt.Printf("  在 %s 末尾追加了 3 个垃圾字节\n", filepath.Base(last))
	l, err = storage.Open(dir, cfg) // 重启：会打印一行「truncating」日志
	check(err)
	defer l.Close()
	fmt.Printf("  重启成功，下一个 offset 仍然是 %d，半写的数据已被截断\n", l.NextOffset())
	showState(l)
}

func showState(l *storage.Log) {
	state := map[string]string{}
	for o := l.OldestOffset(); o < l.NextOffset(); o++ {
		r, err := l.Read(o)
		if err != nil {
			continue // 被压缩掉的 offset
		}
		if r.Tombstone {
			delete(state, string(r.Key))
		} else {
			state[string(r.Key)] = string(r.Value)
		}
	}
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %s = %s\n", k, state[k])
	}
}

func showFiles(dir string) {
	fmt.Println("  磁盘上的文件：")
	entries, err := os.ReadDir(dir)
	check(err)
	for _, e := range entries {
		info, err := e.Info()
		check(err)
		fmt.Printf("    %-28s %4d 字节\n", e.Name(), info.Size())
	}
}

func lastLogFile(dir string) string {
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	check(err)
	sort.Strings(files)
	return files[len(files)-1]
}

func step(title string) { fmt.Println("\n==", title) }

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
