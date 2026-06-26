package server

import (
	"errors"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// checkErrorResp 验证 resp 是 *proto.Error，失败则停止测试
func checkErrorResp(t *testing.T, resp proto.RESP) {
	t.Helper()
	_, ok := resp.(*proto.Error)
	if !ok {
		t.Fatalf("expected *proto.Error, got %T", resp)
	}
}

// TestErrorInjection_GetHandlerError 验证 store.Get 报错时 handler 返回 RESP Error
func TestErrorInjection_GetHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// 先写入一个正常 key
	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("ek1"), []byte("ev1")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// 注入 store.Get 错误
	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("Get", errors.New("store error: disk full"))

	// GET 命令应返回 RESP Error
	resp = handler.executeCommand(state, "GET", [][]byte{[]byte("ek1")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}

// TestErrorInjection_SetHandlerError 验证 store.Set 报错时 handler 返回 RESP Error
func TestErrorInjection_SetHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("Set", errors.New("store error: read-only"))

	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("k"), []byte("v")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}

// TestErrorInjection_DelHandlerError 验证 store.Del 报错时 handler 返回 RESP Error
func TestErrorInjection_DelHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// 先写入 key
	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("delme"), []byte("v")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("Del", errors.New("store error: del failed"))

	resp = handler.executeCommand(state, "DEL", [][]byte{[]byte("delme")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}

// TestErrorInjection_ExpireHandlerError 验证 store.Expire 报错时 handler 返回 RESP Error
func TestErrorInjection_ExpireHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	// 先写入 key
	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("exk"), []byte("v")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("Expire", errors.New("store error: expire failed"))

	resp = handler.executeCommand(state, "EXPIRE", [][]byte{[]byte("exk"), []byte("100")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}

// TestErrorInjection_HSetHandlerError 验证 store.HSet 报错时 handler 返回 RESP Error
func TestErrorInjection_HSetHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("HSet", errors.New("store error: hset failed"))

	resp := handler.executeCommand(state, "HSET", [][]byte{[]byte("h"), []byte("f"), []byte("v")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}

// TestErrorInjection_ZAddHandlerError 验证 store.ZAdd 报错时 handler 返回 RESP Error
func TestErrorInjection_ZAddHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("ZAdd", errors.New("store error: zadd failed"))

	resp := handler.executeCommand(state, "ZADD", [][]byte{[]byte("z"), []byte("1"), []byte("m")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}

// TestErrorInjection_LPushHandlerError 验证 store.LPush 报错时 handler 返回 RESP Error
func TestErrorInjection_LPushHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("LPush", errors.New("store error: lpush failed"))

	resp := handler.executeCommand(state, "LPUSH", [][]byte{[]byte("l"), []byte("v")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}

// TestErrorInjection_SAddHandlerError 验证 store.SAdd 报错时 handler 返回 RESP Error
func TestErrorInjection_SAddHandlerError(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("SAdd", errors.New("store error: sadd failed"))

	resp := handler.executeCommand(state, "SADD", [][]byte{[]byte("s"), []byte("m")}, "127.0.0.1:12345")
	checkErrorResp(t, resp)
}
