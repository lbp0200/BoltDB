package helper

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// store/badger_store.go
func Uint64ToBytes(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

func BytesToUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func ProtectGoroutine(goFunc func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Logger.Error().Interface("error", err).Msg("ProtectGoroutine: recovered from panic")
			}
		}()
		goFunc()
	}()
}

// BytesToFloat64 将字节数组转换为 float64
func BytesToFloat64(b []byte) (float64, error) {
	if len(b) != 8 {
		return 0, errors.New("input must be exactly 8 bytes")
	}
	buf := bytes.NewBuffer(b)
	var f float64
	err := binary.Read(buf, binary.LittleEndian, &f)
	return f, err
}

// Float64ToBytes 将 float64 转换为字节数组
func Float64ToBytes(f float64) []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, f)
	return buf.Bytes()
}
func InterfaceToBytes(data interface{}) ([]byte, error) {
	logFuncTag := "InterfaceToBytes"
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, fmt.Errorf("%s,Encode Error:%v", logFuncTag, err)
	}
	return buf.Bytes(), nil
}
