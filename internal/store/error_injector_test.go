package store

import (
	"errors"
	"testing"

	"github.com/zeebo/assert"
)

// TestErrorInjector_Basic 验证注入/清除/恢复周期
func TestErrorInjector_Basic(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	// 先写入一个正常 key
	assert.NoError(t, s.Set("normal", "value"))

	// 注入错误
	injectErr := errors.New("injected: Get failed")
	injector.Set("Get", injectErr)

	// Get 应返回注入的错误
	_, err := s.Get("normal")
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())

	// 其他方法不受影响
	assert.NoError(t, s.Set("other", "ok"))

	// 清除后恢复正常
	injector.Clear()
	val, err := s.Get("normal")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)
}

// TestErrorInjector_Set 验证 Set 注入
func TestErrorInjector_Set(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: Set failed")
	injector.Set("Set", injectErr)

	err := s.Set("any", "val")
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_Del 验证 Del 注入
func TestErrorInjector_Del(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: Del failed")
	injector.Set("Del", injectErr)

	_, err := s.Del("any")
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_HSet 验证 HSet 注入
func TestErrorInjector_HSet(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: HSet failed")
	injector.Set("HSet", injectErr)

	err := s.HSet("h", "f", "v")
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_HGet 验证 HGet 注入
func TestErrorInjector_HGet(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	// 先写入正常 hash
	assert.NoError(t, s.HSet("h", "f", "v"))

	injectErr := errors.New("injected: HGet failed")
	injector.Set("HGet", injectErr)

	_, err := s.HGet("h", "f")
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_ZAdd 验证 ZAdd 注入
func TestErrorInjector_ZAdd(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: ZAdd failed")
	injector.Set("ZAdd", injectErr)

	err := s.ZAdd("z", []ZSetMember{{Member: "a", Score: 1}})
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_LPush 验证 LPush 注入
func TestErrorInjector_LPush(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: LPush failed")
	injector.Set("LPush", injectErr)

	_, err := s.LPush("l", "a")
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_SAdd 验证 SAdd 注入
func TestErrorInjector_SAdd(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: SAdd failed")
	injector.Set("SAdd", injectErr)

	_, err := s.SAdd("setkey", "m1")
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_Expire 验证 Expire 注入
func TestErrorInjector_Expire(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: Expire failed")
	injector.Set("Expire", injectErr)

	_, err := s.Expire("nonexistent", 10)
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_SetStringBatch 验证 SetStringBatch 注入
func TestErrorInjector_SetStringBatch(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	injectErr := errors.New("injected: SetStringBatch failed")
	injector.Set("SetStringBatch", injectErr)

	err := s.SetStringBatch([]StringEntry{{Key: "k", Value: "v"}})
	assert.Error(t, err)
	assert.Equal(t, injectErr.Error(), err.Error())
}

// TestErrorInjector_NilInjector 验证 nil injector 时 check 不产生副作用
func TestErrorInjector_NilInjector(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	// 默认 errorInjector 为 nil，所有操作应正常
	assert.NoError(t, s.Set("k", "v"))
	val, err := s.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)
}

// TestErrorInjector_UnusedMethod 验证未注入的方法不受影响
func TestErrorInjector_UnusedMethod(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	// 只为 Get 注入，Set 不受影响
	injector.Set("Get", errors.New("get error"))
	assert.NoError(t, s.Set("k", "v"))

	// Get 受影响
	_, err := s.Get("k")
	assert.Error(t, err)

	// 清除后 Get 恢复
	injector.Clear()
	val, err := s.Get("k")
	assert.NoError(t, err)
	assert.Equal(t, "v", val)
}

// TestErrorInjector_Concurrent 验证并发安全
func TestErrorInjector_Concurrent(t *testing.T) {
	t.Parallel()
	s := setupTestStore(t)

	injector := NewErrorInjector()
	s.SetErrorInjector(injector)
	defer s.SetErrorInjector(nil)

	const goroutines = 20
	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			injector.Set("Get", errors.New("err"))
			injector.Clear()
			_, err := s.Get("k")
			_ = err // 并发安全，不 panic 即可
			done <- true
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
}
