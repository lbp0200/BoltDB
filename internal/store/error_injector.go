package store

import (
	"sync"
)

// ErrorInjector 允许在 store 方法中注入错误，用于测试高层错误处理路径。
// 线程安全，零开销（nil check）当未激活时。
type ErrorInjector struct {
	mu     sync.RWMutex
	errors map[string]error // method name → 注入的错误
}

// NewErrorInjector 创建一个新的 ErrorInjector
func NewErrorInjector() *ErrorInjector {
	return &ErrorInjector{
		errors: make(map[string]error),
	}
}

// Set 为指定方法注入错误。之后每次调用该方法都会返回此错误，直到被 Clear。
func (ei *ErrorInjector) Set(method string, err error) {
	ei.mu.Lock()
	defer ei.mu.Unlock()
	ei.errors[method] = err
}

// Clear 清除所有已注入的错误。
func (ei *ErrorInjector) Clear() {
	ei.mu.Lock()
	defer ei.mu.Unlock()
	ei.errors = make(map[string]error)
}

// Check 检查指定方法是否有活跃的注入错误。无注入时返回 nil。
func (ei *ErrorInjector) Check(method string) error {
	ei.mu.RLock()
	defer ei.mu.RUnlock()
	return ei.errors[method]
}
