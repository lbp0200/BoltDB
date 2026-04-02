package store

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/lbp0200/BoltDB/internal/logger"
	lz4 "github.com/pierrec/lz4/v4"
)

// CompressionType 压缩算法类型
type CompressionType string

const (
	CompressionNone  CompressionType = "none"   // 不压缩
	CompressionLZ4   CompressionType = "lz4"    // LZ4压缩
	CompressionZSTD  CompressionType = "zstd"  // ZSTD压缩
	CompressionSnappy CompressionType = "snappy" // Snappy压缩（默认）
)

// compressionMagic 压缩数据的前缀魔数，用于识别压缩算法
var (
	compressionMagicLZ4    = []byte{0x4C, 0x5A, 0x34, 0x01} // "LZ4\01"
	compressionMagicZSTD   = []byte{0x5A, 0x53, 0x54, 0x44} // "ZSTD"
	compressionMagicSnappy = []byte{0x53, 0x4E, 0x41, 0x50} // "SNAP"
)

// compressData 压缩数据
func compressData(data []byte, compressionType CompressionType) ([]byte, error) {
	if compressionType == CompressionNone || len(data) == 0 {
		return data, nil
	}

	switch compressionType {
	case CompressionLZ4:
		return compressLZ4(data)
	case CompressionZSTD:
		return compressZSTD(data)
	case CompressionSnappy:
		return compressSnappy(data)
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// decompressData 解压缩数据
func decompressData(data []byte) ([]byte, error) {
	if len(data) == 0 {
		// 返回非空的空切片，避免 nil 问题
		return []byte{}, nil
	}

	// 检查是否有压缩标记
	if len(data) >= len(compressionMagicLZ4) {
		if bytes.HasPrefix(data, compressionMagicLZ4) {
			return decompressLZ4(data[len(compressionMagicLZ4):])
		}
		if bytes.HasPrefix(data, compressionMagicZSTD) {
			return decompressZSTD(data[len(compressionMagicZSTD):])
		}
		if bytes.HasPrefix(data, compressionMagicSnappy) {
			return decompressSnappy(data[len(compressionMagicSnappy):])
		}
	}

	// 没有压缩标记，返回原始数据
	return data, nil
}

// compressLZ4 使用LZ4压缩
func compressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	
	// 设置压缩级别（可选，lz4默认是快速压缩）
	// writer.Header.CompressionLevel = lz4.Level1
	
	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("lz4 compress write error: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("lz4 compress close error: %w", err)
	}

	// 添加压缩标记
	result := make([]byte, len(compressionMagicLZ4)+buf.Len())
	copy(result, compressionMagicLZ4)
	copy(result[len(compressionMagicLZ4):], buf.Bytes())
	
	return result, nil
}

// decompressLZ4 使用LZ4解压缩
func decompressLZ4(data []byte) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(data))
	var buf bytes.Buffer
	
	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("lz4 decompress error: %w", err)
	}
	
	return buf.Bytes(), nil
}

// compressZSTD 使用ZSTD压缩
func compressZSTD(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("zstd encoder creation error: %w", err)
	}
	defer func() {
		if err := encoder.Close(); err != nil {
			logger.Logger.Debug().Err(err).Msg("failed to close zstd encoder")
		}
	}()

	compressed := encoder.EncodeAll(data, nil)
	
	// 添加压缩标记
	result := make([]byte, len(compressionMagicZSTD)+len(compressed))
	copy(result, compressionMagicZSTD)
	copy(result[len(compressionMagicZSTD):], compressed)
	
	return result, nil
}

// decompressZSTD 使用ZSTD解压缩
func decompressZSTD(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decoder creation error: %w", err)
	}
	defer decoder.Close()

	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress error: %w", err)
	}
	
	return decompressed, nil
}

// compressSnappy 使用Snappy压缩
func compressSnappy(data []byte) ([]byte, error) {
	compressed := snappy.Encode(nil, data)

	// 添加压缩标记
	result := make([]byte, len(compressionMagicSnappy)+len(compressed))
	copy(result, compressionMagicSnappy)
	copy(result[len(compressionMagicSnappy):], compressed)

	return result, nil
}

// decompressSnappy 使用Snappy解压缩
func decompressSnappy(data []byte) ([]byte, error) {
	decompressed, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("snappy decompress error: %w", err)
	}

	return decompressed, nil
}

// shouldCompress 判断是否应该压缩数据
// 对于小数据，压缩可能反而增加大小，所以设置一个阈值
func shouldCompress(data []byte, compressionType CompressionType) bool {
	if compressionType == CompressionNone {
		return false
	}
	// 小于64字节的数据不压缩，压缩开销可能大于收益
	return len(data) >= 64
}

// setValueWithCompression 带压缩的数据写入辅助函数
func (s *BotreonStore) setValueWithCompression(txn *badger.Txn, key []byte, value []byte) error {
	if shouldCompress(value, s.compressionType) {
		compressed, err := compressData(value, s.compressionType)
		if err != nil {
			return fmt.Errorf("compression error: %w", err)
		}
		// 如果压缩后反而更大，使用原始数据
		if len(compressed) >= len(value) {
			return txn.Set(key, value)
		}
		return txn.Set(key, compressed)
	}
	return txn.Set(key, value)
}

// setEntryWithCompression 带压缩的Entry写入辅助函数
func (s *BotreonStore) setEntryWithCompression(txn *badger.Txn, key []byte, value []byte, ttl time.Duration) error {
	if shouldCompress(value, s.compressionType) {
		compressed, err := compressData(value, s.compressionType)
		if err != nil {
			return fmt.Errorf("compression error: %w", err)
		}
		// 如果压缩后反而更大，使用原始数据
		if len(compressed) >= len(value) {
			if ttl > 0 {
				e := badger.NewEntry(key, value).WithTTL(ttl)
				return txn.SetEntry(e)
			}
			return txn.Set(key, value)
		}
		// 使用压缩后的值
		if ttl > 0 {
			e := badger.NewEntry(key, compressed).WithTTL(ttl)
			return txn.SetEntry(e)
		}
		return txn.Set(key, compressed)
	}
	// 不压缩
	if ttl > 0 {
		e := badger.NewEntry(key, value).WithTTL(ttl)
		return txn.SetEntry(e)
	}
	return txn.Set(key, value)
}

// getValueWithDecompression 带解压缩的数据读取辅助函数
func (s *BotreonStore) getValueWithDecompression(item *badger.Item) ([]byte, error) {
	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return decompressData(value)
}

