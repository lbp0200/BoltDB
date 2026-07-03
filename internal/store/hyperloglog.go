package store

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"

	"github.com/dgraph-io/badger/v4"
)

// HyperLogLog 实现（基于 14 位寄存器的密集编码）
const (
	hllRegisterBits  = 14
	hllRegisterCount = 1 << hllRegisterBits // 16384
	hllRegisterMask  = hllRegisterCount - 1
	// hllAlpha 是 HyperLogLog 的偏置校正常数 α_m。
	// 对于 m >= 128: α_m = 0.721347520444481703739965215 / (1 + 1.079/m)。
	// m = 16384 时 α ≈ 0.721300, 在此直接使用 ∞ 近似值足够精确（偏差 < 0.007%）。
	hllAlpha = 0.721347520444481703739965215
)

// HyperLogLog 结构
type HyperLogLog struct {
	registers []uint8 // 每个寄存器 6 位 (存储 count)
	encoding  byte    // 0=未初始化, 1=稀疏, 2=密集
}

// encodeRegister 将 6 位 count 编码到字节
func encodeRegister(count uint8) byte {
	return count & 0x3F // 6 位掩码
}

// decodeRegister 从字节解码 count
func decodeRegister(b byte) uint8 {
	return b & 0x3F // 取低 6 位
}

// newHyperLogLog 创建新的 HyperLogLog
func newHyperLogLog() *HyperLogLog {
	return &HyperLogLog{
		encoding: 1, // 稀疏编码
	}
}

func (h *HyperLogLog) Estimate() float64 {
	if h.encoding == 0 {
		return 0
	}

	sum := 0.0
	if h.encoding == 2 {
		for _, reg := range h.registers {
			count := decodeRegister(reg)
			sum += 1.0 / float64(uint64(1)<<count)
		}
	} else {
		return 0
	}

	if sum == 0 {
		return 0
	}

	// 标准 HLL 估计: E = α_m * m² / Σ 2^{-M[j]}
	estimate := hllAlpha * float64(hllRegisterCount) * float64(hllRegisterCount) / sum

	// 小基数校正: 当 raw estimate 低于 5m/2 阈值时使用线性计数
	// 标准 HLL 论文 (Flajolet+ 2007): 若 E ≤ 5m/2 且存在零寄存器，线性计数更精确
	const linearThreshold = 5 * hllRegisterCount / 2 // 5m/2 = 40960
	if estimate <= linearThreshold {
		if zeros := h.countZeros(); zeros > 0 {
			linearCount := float64(hllRegisterCount) * math.Log(float64(hllRegisterCount)/float64(zeros))
			return linearCount
		}
	}
	return estimate
}

// countZeros 计算零寄存器数量（用于线性计数）
func (h *HyperLogLog) countZeros() int {
	zeros := 0
	for _, reg := range h.registers {
		if decodeRegister(reg) == 0 {
			zeros++
		}
	}
	return zeros
}

// add 添加元素，返回是否发生了变化
func (h *HyperLogLog) add(data []byte) bool {
	// 初始化寄存器（如果尚未初始化）
	if len(h.registers) == 0 {
		h.encoding = 2 // 使用密集编码
		h.registers = make([]byte, hllRegisterCount)
	}

	// 计算哈希值
	hashVal := hashData(data)

	// 使用前 14 位作为寄存器索引
	registerIdx := hashVal & hllRegisterMask

	// 计算尾随零的数量 + 1（使用 CPU 指令级 bits.TrailingZeros64）
	tz := bits.TrailingZeros64(hashVal >> hllRegisterBits)
	count := tz + 1
	if count > 63 {
		count = 63 // 最大值
	}

	// 更新寄存器
	oldCount := decodeRegister(h.registers[registerIdx])
	if count > int(oldCount) {
		h.registers[registerIdx] = encodeRegister(uint8(count))
		return true
	}
	return false
}

// hashData 计算数据的哈希值
func hashData(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

// merge 合并另一个 HyperLogLog
func (h *HyperLogLog) merge(other *HyperLogLog) bool {
	changed := false
	if h.encoding != other.encoding {
		// 确保编码一致
		if other.encoding == 0 {
			return false
		}
		// 转换为密集编码
		h.encoding = 2
		h.registers = make([]byte, hllRegisterCount)
	}

	for i := 0; i < hllRegisterCount; i++ {
		otherCount := decodeRegister(other.registers[i])
		hCount := decodeRegister(h.registers[i])
		if otherCount > hCount {
			h.registers[i] = encodeRegister(otherCount)
			changed = true
		}
	}
	return changed
}

// PFAdd 实现 Redis PFADD 命令
func (s *BotreonStore) PFAdd(key string, elements ...string) (int64, error) {
	var changed int64

	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 设置类型
		typeKey := TypeOfKeyGet(key)
		if err := txn.Set(typeKey, []byte("hyperloglog")); err != nil {
			return err
		}

		// 获取或创建 HyperLogLog
		hll, err := s.getOrCreateHLL(txn, key)
		if err != nil {
			return err
		}

		// 添加元素
		for _, elem := range elements {
			if hll.add([]byte(elem)) {
				changed = 1
			}
		}

		// 保存
		return s.saveHLL(txn, key, hll)
	}, 30)

	return changed, err
}

// getOrCreateHLL 获取或创建 HyperLogLog
func (s *BotreonStore) getOrCreateHLL(txn *badger.Txn, key string) (*HyperLogLog, error) {
	hllKey := []byte(fmt.Sprintf("%s%s", HyperLogLogPrefix, key))

	var hll *HyperLogLog
	item, err := txn.Get(hllKey)
	if err == nil {
		// 存在，读取
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		hll = &HyperLogLog{
			encoding:  val[0],
			registers: val[1:],
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, err
	} else {
		// 不存在，创建新的
		hll = newHyperLogLog()
	}

	return hll, nil
}

// saveHLL 保存 HyperLogLog
func (s *BotreonStore) saveHLL(txn *badger.Txn, key string, hll *HyperLogLog) error {
	hllKey := []byte(fmt.Sprintf("%s%s", HyperLogLogPrefix, key))
	data := make([]byte, 1+len(hll.registers))
	data[0] = hll.encoding
	copy(data[1:], hll.registers)
	return txn.Set(hllKey, data)
}

// PFCount 实现 Redis PFCOUNT 命令
func (s *BotreonStore) PFCount(keys ...string) (int64, error) {
	if len(keys) == 1 {
		return s.pfCountOne(keys[0])
	}
	return s.pfCountMultiple(keys)
}

// pfCountOne 计算单个 key 的基数
func (s *BotreonStore) pfCountOne(key string) (int64, error) {
	var estimate float64

	err := s.db.View(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			estimate = 0
			return nil
		}
		if err != nil {
			return err
		}

		// 读取 HyperLogLog
		hllKey := []byte(fmt.Sprintf("%s%s", HyperLogLogPrefix, key))
		item, err := txn.Get(hllKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			estimate = 0
			return nil
		}
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(val) == 0 {
			estimate = 0
			return nil
		}

		hll := &HyperLogLog{
			encoding:  val[0],
			registers: val[1:],
		}
		estimate = hll.Estimate()
		return nil
	})

	// 四舍五入到整数
	return int64(math.Round(estimate)), err
}

// pfCountMultiple 计算多个 key 的基数（合并后计算）
func (s *BotreonStore) pfCountMultiple(keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	var merged *HyperLogLog

	err := s.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			// 检查键是否存在
			typeKey := TypeOfKeyGet(key)
			_, err := txn.Get(typeKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			// 读取 HyperLogLog
			hllKey := []byte(fmt.Sprintf("%s%s", HyperLogLogPrefix, key))
			item, err := txn.Get(hllKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}

			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(val) == 0 {
				continue
			}

			other := &HyperLogLog{
				encoding:  val[0],
				registers: val[1:],
			}

			if merged == nil {
				merged = other
			} else {
				merged.merge(other)
			}
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	if merged == nil {
		return 0, nil
	}

	estimate := merged.Estimate()
	return int64(math.Round(estimate)), nil
}

// PFMerge 实现 Redis PFMERGE 命令
func (s *BotreonStore) PFMerge(destKey string, sourceKeys ...string) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		// 设置目标键类型
		typeKey := TypeOfKeyGet(destKey)
		if err := txn.Set(typeKey, []byte("hyperloglog")); err != nil {
			return err
		}

		// 获取或创建目标 HyperLogLog
		destHLL, err := s.getOrCreateHLL(txn, destKey)
		if err != nil {
			return err
		}

		// 合并所有源键
		for _, sourceKey := range sourceKeys {
			sourceHLL, err := s.getOrCreateHLL(txn, sourceKey)
			if err != nil {
				return err
			}
			destHLL.merge(sourceHLL)
		}

		// 保存目标
		return s.saveHLL(txn, destKey, destHLL)
	}, 30)
}

// RestoreHLL 从原始字节恢复 HyperLogLog（FULLRESYNC RDB 加载用）
func (s *BotreonStore) RestoreHLL(key string, data []byte) error {
	return s.retryUpdate(func(txn *badger.Txn) error {
		typeKey := TypeOfKeyGet(key)
		if err := txn.Set(typeKey, []byte(KeyTypeHyperLogLog)); err != nil {
			return err
		}
		hllKey := []byte(fmt.Sprintf("%s%s", HyperLogLogPrefix, key))
		return txn.Set(hllKey, data)
	}, 30)
}

// PFInfo 实现 Redis PFINFO 命令（可选）
func (s *BotreonStore) PFInfo(key string) (map[string]int64, error) {
	info := make(map[string]int64)

	err := s.db.View(func(txn *badger.Txn) error {
		// 检查键是否存在
		typeKey := TypeOfKeyGet(key)
		_, err := txn.Get(typeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("key does not exist")
		}
		if err != nil {
			return err
		}

		// 读取 HyperLogLog
		hllKey := []byte(fmt.Sprintf("%s%s", HyperLogLogPrefix, key))
		item, err := txn.Get(hllKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("key does not exist")
		}
		if err != nil {
			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(val) == 0 {
			return nil
		}

		hll := &HyperLogLog{
			encoding:  val[0],
			registers: val[1:],
		}

		info["encoding"] = int64(hll.encoding)
		info["card"] = int64(math.Round(hll.Estimate()))
		info["size"] = int64(len(val))
		return nil
	})

	return info, err
}
