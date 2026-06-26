package server

import (
	"errors"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// TestErrorInjection_MultiExecErrorPropagation 验证 MULTI/EXEC 中
// 被队列命令的 store 错误能正确传播（c9cd7d3 bug 回归）。
func TestErrorInjection_MultiExecErrorPropagation(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	injector.Set("Set", errors.New("store error: disk full"))

	// MULTI
	resp := handler.executeCommand(state, "MULTI", nil, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))

	// SET inside transaction → QUEUED
	resp = handler.executeCommand(state, "SET", [][]byte{[]byte("k"), []byte("v")}, "127.0.0.1:12345")
	ss, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "QUEUED", string(*ss))

	// EXEC → should return NestedArray containing one Error
	resp = handler.executeCommand(state, "EXEC", nil, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	if !ok {
		t.Fatalf("EXEC should return *proto.NestedArray, got %T", resp)
	}
	if len(na.Elems) != 1 {
		t.Fatalf("expected 1 element, got %d", len(na.Elems))
	}
	_, isErr := na.Elems[0].(*proto.Error)
	if !isErr {
		t.Errorf("first element should be *proto.Error, got %T", na.Elems[0])
	}
}

// TestErrorInjection_MultiExecGetErrorPropagation 验证 GET 在事务中的错误传播。
func TestErrorInjection_MultiExecGetErrorPropagation(t *testing.T) {
	t.Parallel()
	handler, state := setupTestHandler(t)
	defer handler.Db.Close()

	injector := store.NewErrorInjector()
	handler.Db.SetErrorInjector(injector)
	defer handler.Db.SetErrorInjector(nil)

	// 先写入一个正常 key
	resp := handler.executeCommand(state, "SET", [][]byte{[]byte("k"), []byte("v")}, "127.0.0.1:12345")
	assert.Equal(t, proto.OK, resp)

	// 注入 store.Get 错误
	injector.Set("Get", errors.New("store error: read failed"))

	// MULTI
	resp = handler.executeCommand(state, "MULTI", nil, "127.0.0.1:12345")
	ss, ok := resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "OK", string(*ss))

	// GET inside transaction → QUEUED
	resp = handler.executeCommand(state, "GET", [][]byte{[]byte("k")}, "127.0.0.1:12345")
	ss, ok = resp.(*proto.SimpleString)
	assert.True(t, ok)
	assert.Equal(t, "QUEUED", string(*ss))

	// EXEC
	resp = handler.executeCommand(state, "EXEC", nil, "127.0.0.1:12345")
	na, ok := resp.(*proto.NestedArray)
	if !ok {
		t.Fatalf("EXEC should return *proto.NestedArray, got %T", resp)
	}
	if len(na.Elems) != 1 {
		t.Fatalf("expected 1 element, got %d", len(na.Elems))
	}
	_, isErr := na.Elems[0].(*proto.Error)
	if !isErr {
		t.Errorf("first element should be *proto.Error, got %T", na.Elems[0])
	}
}
