# Contributing to BoltDB

感谢你对 BoltDB 的关注！以下是参与贡献的指南。

## 开发环境

- Go 1.25+ (推荐使用最新稳定版)
- Linux/macOS (不支持 Windows 原生开发)
- 远程测试服务器用于 `-race` 测试 (Mac M 系列芯片会死锁)

## 构建

```bash
# 构建二进制文件 (输出到 ./build/)
go build -o ./build/boltDB ./cmd/boltDB/

# 启动开发服务器
go run ./cmd/boltDB/ -dir=/tmp/bolt_db_data
```

## 测试

```bash
# 本地快速测试 (跳过 race 检测)
bash scripts/remote-test.sh -short ./internal/...

# 远程完整测试 (包含 race 检测)
bash scripts/remote-test.sh -race -short ./internal/...

# Linter
golangci-lint run --timeout 5m
```

**重要**: 永远不要在 Mac M 系列芯片上运行 `go test -race`，会导致死锁。所有 `-race` 测试必须通过远程服务器执行。

## 代码规范

- 遵循 Go 标准代码风格
- 新增命令必须包含 RESP Shape 测试 (参考 `internal/server/handler_resp_shape_test.go`)
- 写命令必须添加到 `internal/server/replication_helper.go` 的 `isWriteCommand` map 中
- 提交前确保 `golangci-lint` 通过

## 提交流程

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/my-feature`)
3. 提交更改 (`git commit -m 'Add some feature'`)
4. 推送到分支 (`git push origin feature/my-feature`)
5. 创建 Pull Request

## 报告问题

使用 GitHub Issues 报告 bug，请包含：
- 复现步骤
- 期望行为 vs 实际行为
- 环境信息 (OS, Go 版本, BoltDB 版本)
- 相关日志或错误信息

## 许可证

本项目采用 MIT 许可证。提交贡献即表示你同意将代码以相同许可证发布。
