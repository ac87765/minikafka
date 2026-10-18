# 本地环境搭建（Windows + VS Code）

## 1. 安装 Go

1. 打开 <https://go.dev/dl/>，下载 Windows 安装包（`go1.xx.x.windows-amd64.msi`），一路下一步。
2. 安装完**重新打开**一个终端（PowerShell 或 cmd），输入：

   ```powershell
   go version
   ```

   能看到 `go version go1.22` 或更高的版本号就成功了。

> 国内网络下载依赖慢的话，执行一次：`go env -w GOPROXY=https://goproxy.cn,direct`
> （从阶段 2 开始项目依赖 `google.golang.org/protobuf`，第一次 `go test` 时会自动下载。）

## 2. 安装 VS Code 和 Go 插件

1. 安装 VS Code：<https://code.visualstudio.com/>
2. 安装 Git：<https://git-scm.com/download/win>
3. 克隆项目并用 VS Code 打开：

   ```powershell
   cd D:\work
   git clone https://github.com/ac87765/minikafka.git
   code minikafka
   ```

4. 打开后右下角会提示安装推荐插件 **Go**（作者 Go Team at Google），点安装。
5. 插件装好后会提示安装 Go 工具（gopls、dlv 等），点 **Install All**。
   如果没弹出：`Ctrl+Shift+P` → 输入 `Go: Install/Update Tools` → 全选 → 确定。

## 3. 运行演示程序

演示程序把阶段 1 的功能从头到尾跑一遍：写入、分段、读取、压缩、模拟断电后恢复，
每一步都打印磁盘上的文件。

- **终端里运行：** `` Ctrl+` `` 打开终端，输入 `go run ./cmd/demo`
- **或者按 F5**，选「运行演示 (cmd/demo)」。

## 4. 运行测试

| 想做的事 | 怎么做 |
|---|---|
| 跑全部测试 | 终端输入 `go test ./...`，或 `Ctrl+Shift+P` → `Tasks: Run Test Task` |
| 看每个测试的详情 | `go test -v ./storage` |
| 只跑某一个测试 | `go test -v -run TestCompactionPreservesState ./storage` |
| 点一下就跑 | 打开 `*_test.go` 文件，每个 `func TestXxx` 上方有灰色的 **run test / debug test**，点它 |
| 测试面板 | 左侧栏烧杯图标（Testing），能看到所有测试，逐个运行 |
| 竞态检测 | `go test -race ./...`（Windows 上需要装 gcc，没装就跳过这一项） |

## 5. 单步调试（理解代码最好的方法）

1. 打开 `storage/compact_test.go`。
2. 在 `TestCompactionKeepsLatestAndOffsets` 里某一行的行号左边点一下，出现红点（断点）。
3. 点函数上方的 **debug test**。
4. 程序停在断点处，左侧能看到所有变量的值。`F10` 下一行，`F11` 进入函数，`F5` 继续。

推荐跟着调试看一遍的流程：

- `Log.Append` → `segment.append` → `Encode`：一条记录怎么变成字节
- `Log.Read` → `index.lookup` → `segment.read`：两级查找
- `openSegment` → `rebuild`：重启时怎么发现并截断半写记录
- `Log.Compact` → `rewriteSegment`：压缩怎么安全地换掉旧文件

## 6. 目录结构

```
minikafka/
├── storage/          阶段 1：单机存储引擎
│   ├── record.go     记录格式、编解码、CRC 校验
│   ├── index.go      稀疏索引
│   ├── segment.go    单个段：.log + .index，崩溃恢复
│   ├── log.go        多个段组成的日志：滚动、读取、按时间删除
│   ├── compact.go    日志压缩
│   └── *_test.go     测试
├── cmd/demo/         演示程序
├── lessons/          课程笔记
└── .vscode/          VS Code 配置（推荐插件、调试配置、测试任务）
```
