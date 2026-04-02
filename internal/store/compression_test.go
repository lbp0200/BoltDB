package store

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// BenchmarkCompression 对比各压缩算法的性能
func BenchmarkCompression(b *testing.B) {
	// 使用较大的数据块进行压缩测试
	largeValue := strings.Repeat("This is a test string that will be compressed. ", 100)
	data := []byte(largeValue)

	testCases := []struct {
		name      string
		algo      CompressionType
	}{
		{"LZ4", CompressionLZ4},
		{"Snappy", CompressionSnappy},
		{"ZSTD", CompressionZSTD},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("Compress", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					compressed, err := compressData(data, tc.algo)
					if err != nil {
						b.Fatal(err)
					}
					// 防止编译器优化
					_ = len(compressed)
				}
			})

			compressed, err := compressData(data, tc.algo)
			if err != nil {
				b.Fatal(err)
			}

			b.Run("Decompress", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					decompressed, err := decompressData(compressed)
					if err != nil {
						b.Fatal(err)
					}
					// 防止编译器优化
					_ = len(decompressed)
				}
			})
		})
	}
}

// BenchmarkCompressionRatio 对比各算法的压缩率
func BenchmarkCompressionRatio(b *testing.B) {
	testCases := []struct {
		name string
		algo CompressionType
	}{
		{"LZ4", CompressionLZ4},
		{"Snappy", CompressionSnappy},
		{"ZSTD", CompressionZSTD},
	}

	dataSizes := []int{1024, 10240, 102400} // 1KB, 10KB, 100KB

	for _, size := range dataSizes {
		// 生成不同大小的测试数据
		var data []byte
		for i := 0; i < size; i++ {
			data = append(data, byte('A'+i%26))
		}

		for _, tc := range testCases {
			tc := tc
			size := size
			b.Run(fmt.Sprintf("%s-%dB", tc.name, size), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					compressed, err := compressData(data, tc.algo)
					if err != nil {
						b.Fatal(err)
					}
					ratio := float64(len(compressed)) / float64(len(data))
					// 防止编译器优化
					_ = ratio
				}
			})
		}
	}
}

// BenchmarkStoreCompression 带存储的压缩性能测试
func BenchmarkStoreCompression(b *testing.B) {
	testCases := []struct {
		name string
		algo CompressionType
	}{
		{"LZ4", CompressionLZ4},
		{"Snappy", CompressionSnappy},
		{"ZSTD", CompressionZSTD},
	}

	for _, tc := range testCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			dbPath := b.TempDir()
			store, err := NewBotreonStoreWithCompression(dbPath, tc.algo)
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()

			largeValue := strings.Repeat("This is a test string that will be compressed. ", 100)
			key := "bench_key"

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := store.Set(key, largeValue)
				if err != nil {
					b.Fatal(err)
				}
				_, err = store.Get(key)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestCompressionLZ4(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBotreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 测试大字符串压缩
	largeValue := strings.Repeat("This is a test string that will be compressed. ", 100)
	key := "large_key"

	// 写入
	err = store.Set(key, largeValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value)
}

func TestCompressionZSTD(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBotreonStoreWithCompression(dbPath, CompressionZSTD)
	assert.NoError(t, err)
	defer store.Close()

	// 测试大字符串压缩
	largeValue := strings.Repeat("This is a test string that will be compressed. ", 100)
	key := "large_key"

	// 写入
	err = store.Set(key, largeValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value)
}

func TestCompressionNone(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBotreonStoreWithCompression(dbPath, CompressionNone)
	assert.NoError(t, err)
	defer store.Close()

	// 测试不压缩
	largeValue := strings.Repeat("This is a test string. ", 100)
	key := "large_key"

	// 写入
	err = store.Set(key, largeValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value)
}

func TestCompressionDefaultIsSnappy(t *testing.T) {
	dbPath := t.TempDir()
	// 不指定压缩算法，使用默认
	store, err := NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer store.Close()

	// 验证默认压缩算法是 Snappy
	assert.Equal(t, CompressionSnappy, store.GetCompression())
}

func TestCompressionSmallData(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBotreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 小数据（小于64字节）不应该被压缩
	smallValue := "small"
	key := "small_key"

	// 写入
	err = store.Set(key, smallValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, smallValue, value)
}

func TestCompressionHash(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBotreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 测试Hash压缩
	key := "user:1"
	largeValue := strings.Repeat("This is a large hash field value. ", 50)

	err = store.HSet(key, "description", largeValue)
	assert.NoError(t, err)

	value, err := store.HGet(key, "description")
	assert.NoError(t, err)
	assert.Equal(t, largeValue, string(value))
}

func TestCompressionBackwardCompatibility(t *testing.T) {
	dbPath := t.TempDir()
	
	// 先不使用压缩写入数据
	store1, err := NewBotreonStoreWithCompression(dbPath, CompressionNone)
	assert.NoError(t, err)
	err = store1.Set("key1", "value1")
	assert.NoError(t, err)
	store1.Close()

	// 使用压缩读取旧数据
	store2, err := NewBotreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store2.Close()

	value, err := store2.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", value)

	// 写入新数据（会被压缩）
	err = store2.Set("key2", strings.Repeat("large value ", 100))
	assert.NoError(t, err)

	// 读取新数据
	value2, err := store2.Get("key2")
	assert.NoError(t, err)
	assert.True(t, len(value2) > 0)
}

func TestCompressionSnappy(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBotreonStoreWithCompression(dbPath, CompressionSnappy)
	assert.NoError(t, err)
	defer store.Close()

	// 测试大字符串压缩
	largeValue := strings.Repeat("This is a test string that will be compressed. ", 100)
	key := "large_key"

	// 写入
	err = store.Set(key, largeValue)
	assert.NoError(t, err)

	// 读取
	value, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value)
}

func TestCompressionSwitch(t *testing.T) {
	dbPath := t.TempDir()
	store, err := NewBotreonStoreWithCompression(dbPath, CompressionLZ4)
	assert.NoError(t, err)
	defer store.Close()

	// 使用LZ4写入
	largeValue := strings.Repeat("test ", 100)
	err = store.Set("key1", largeValue)
	assert.NoError(t, err)

	// 切换到ZSTD
	store.SetCompression(CompressionZSTD)
	assert.Equal(t, CompressionZSTD, store.GetCompression())

	// 使用ZSTD写入
	err = store.Set("key2", largeValue)
	assert.NoError(t, err)

	// 读取两个键都应该正常
	value1, err := store.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value1)

	value2, err := store.Get("key2")
	assert.NoError(t, err)
	assert.Equal(t, largeValue, value2)
}

