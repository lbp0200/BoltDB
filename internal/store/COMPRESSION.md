# 数据压缩功能说明

## 概述

BoltDB现在支持在数据写入和读取时进行压缩/解压缩，支持LZ4和ZSTD两种压缩算法，默认使用LZ4。

## 依赖安装

在使用压缩功能之前，需要安装以下依赖：

```bash
go get github.com/pierrec/lz4/v4
go get github.com/klauspost/compress/zstd
```

或者直接运行：

```bash
go mod tidy
```

## 使用方法

### 1. 使用默认LZ4压缩（推荐）

```go
store, err := store.NewBoltDBStore("/path/to/db")
// 或者
store, err := store.NewBadgerStore("/path/to/db")
```

### 2. 指定压缩算法

```go
// 使用LZ4压缩
store, err := store.NewBoltDBStoreWithCompression("/path/to/db", store.CompressionLZ4)

// 使用ZSTD压缩
store, err := store.NewBoltDBStoreWithCompression("/path/to/db", store.CompressionZSTD)

// 不使用压缩
store, err := store.NewBoltDBStoreWithCompression("/path/to/db", store.CompressionNone)
```

### 3. 运行时修改压缩算法

```go
// 获取当前压缩算法
compressionType := store.GetCompression()

// 设置新的压缩算法
store.SetCompression(store.CompressionZSTD)
```

## 压缩策略

- **自动压缩**：只有当数据大小 >= 64字节时才会压缩，小数据压缩可能反而增加存储空间
- **智能回退**：如果压缩后的数据比原始数据更大，会自动使用原始数据
- **向后兼容**：可以读取未压缩的历史数据，自动检测并解压缩

## 压缩算法对比

### LZ4（默认）
- **优点**：压缩/解压速度快，CPU占用低
- **缺点**：压缩率相对较低
- **适用场景**：对性能要求高的场景，实时数据处理

### ZSTD
- **优点**：压缩率高，压缩率可调
- **缺点**：压缩速度相对较慢，CPU占用较高
- **适用场景**：对存储空间要求高的场景，批量数据处理

## 压缩标记

压缩后的数据会在开头添加魔数标记：
- LZ4: `0x4C 0x5A 0x34 0x01` ("LZ4\01")
- ZSTD: `0x5A 0x53 0x54 0x44` ("ZSTD")

读取时会自动检测这些标记并解压缩。

## 性能影响

- **写入性能**：压缩会增加少量CPU开销，但可以减少I/O操作
- **读取性能**：解压缩开销很小，LZ4解压速度极快
- **存储空间**：根据数据特征，可以节省20%-80%的存储空间

## 注意事项

1. **兼容性**：已存在的数据不会被自动压缩，只有新写入的数据才会压缩
2. **阈值**：小于64字节的数据不会压缩，避免压缩开销大于收益
3. **类型键**：类型键（TYPE_*）不会被压缩，只有实际数据值会被压缩
4. **元数据**：计数器、长度等元数据不会被压缩

## 示例

```go
package main

import (
    "fmt"
    "github.com/lbp0200/BoltDB/internal/store"
)

func main() {
    // 使用LZ4压缩（默认）
    db, err := store.NewBoltDBStore("/tmp/boltDB")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // 写入数据（自动压缩）
    db.Set("large_key", "这是一个很长的字符串，会被自动压缩...")

    // 读取数据（自动解压缩）
    value, err := db.Get("large_key")
    if err != nil {
        panic(err)
    }
    fmt.Println(value)

    // 切换到ZSTD压缩
    db.SetCompression(store.CompressionZSTD)
    db.Set("another_key", "另一个很长的字符串，使用ZSTD压缩...")
}
```

