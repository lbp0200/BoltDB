package replication

import (
	"testing"

	"github.com/zeebo/assert"
)

// TestLoadRDBFailFast_UnknownTypeByte 守卫 §5 fail-fast：载入侧遇到未知数据类型字节时
// 必须返回 error，而不是旧实现的静默 `default: return nil`（会报"载入成功"却已 decoder desync）。
//
// distinguishing guard（pre-fix RED / post-fix GREEN）：
//   - pre-fix（b26523e^）rdb_loader.go 的 switch default 为 `return nil` → LoadRDBWithStore 返回 nil，
//     本测试断言 Error 失败（RED）。
//   - post-fix default 为 `return fmt.Errorf(...)` → 返回 error（GREEN）。
//
// 构造的字节流在 switch 处即返回，不触及尾部 CRC64 块——因此该用例不受既有 CRC 兜底掩盖，
// 是纯"未知类型 fail-fast"的判别输入。
func TestLoadRDBFailFast_UnknownTypeByte(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)
	defer s.Close()

	// 手工构造最小 RDB：REDIS0009 header + [typeByte=0x0A(未知)][key len=1][key='k']。
	// 解码路径：readExpireTime peek 0x0A（非 0xFC/0xFD）→ UnreadByte 还原 → (0,nil)；
	// typeByte=ReadByte()=0x0A；key=readString()="k"；switch 0x0A → default。
	stream := []byte("REDIS0009") // = RDBMagicString + RDBVersion（9 字节）
	stream = append(stream, 0x0A, 0x01, 'k')

	err := LoadRDBWithStore(stream, s)
	assert.Error(t, err) // post-fix：未知类型 fail-fast；pre-fix（b26523e^）此处为 nil → RED
}

// TestLoadRDBFailFast_HealthyStringLoads 基线守卫：健康 RDB 仍应无错载入并 roundtrip，
// 防止"fail-fast"被过度收紧（例如让合法字符串也走 default 报错）。与上面的损坏用例成对：
// 一个证明"该报的报了"，一个证明"不该报的没报"。
func TestLoadRDBFailFast_HealthyStringLoads(t *testing.T) {
	t.Parallel()
	src := setupTestStore(t)
	defer src.Close()

	src.Set("ff:key", "ff:value")

	rdbData, err := GenerateRDB(src)
	assert.NoError(t, err)
	assert.True(t, len(rdbData) > 0)

	dst := setupTestStore(t)
	defer dst.Close()

	err = LoadRDBWithStore(rdbData, dst)
	assert.NoError(t, err)

	val, err := dst.Get("ff:key")
	assert.NoError(t, err)
	assert.Equal(t, "ff:value", val)
}
