package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// JSONValue represents a JSON value stored in the database
type JSONValue struct {
	Data interface{}
}

// JSONPathResult represents the result of a JSONPath query
type JSONPathResult struct {
	Path  string
	Value interface{}
}

// jsonKey generates the key for storing JSON data
func (s *BotreonStore) jsonKey(key string) string {
	return fmt.Sprintf("%s%s", prefixKeyJSONBytes, key)
}

// jsonKeyExistsInTxn reports whether the JSON value key exists.
func (s *BotreonStore) jsonKeyExistsInTxn(txn *badger.Txn, key string) (bool, error) {
	_, err := txn.Get([]byte(s.jsonKey(key)))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

// jsonReadBytesInTxn reads JSON payload inside an open update transaction.
func (s *BotreonStore) jsonReadBytesInTxn(txn *badger.Txn, key string) ([]byte, error) {
	if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
		return nil, err
	}
	item, err := txn.Get([]byte(s.jsonKey(key)))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

// JSONSet implements JSON.SET command
// JSON.SET key path value [NX | XX]
// 返回 (OK, 是否实际写入, 错误)。NX/XX 条件不满足时返回 (OK, false, nil) ——
// 上层借此决定是否污染 WATCH（空转不脏，见 812ab66）以及是否进入复制流。
func (s *BotreonStore) JSONSet(key, path, value string, nx, xx bool) (string, bool, error) {
	// Only support root path for now
	if path != "$" && path != "." {
		return "", false, errors.New("ERR path must be '$' or '.'")
	}

	// Validate JSON first
	var newValue interface{}
	if err := json.Unmarshal([]byte(value), &newValue); err != nil {
		return "", false, errors.New("ERR invalid JSON")
	}

	jsonData, err := json.Marshal(newValue)
	if err != nil {
		return "", false, err
	}

	wrote := false
	err = s.retryUpdate(func(txn *badger.Txn) error {
		exists, err := s.jsonKeyExistsInTxn(txn, key)
		if err != nil {
			return err
		}
		if nx && exists {
			return nil
		}
		if xx && !exists {
			return nil
		}

		item, err := txn.Get(TypeOfKeyGet(key))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeJSON {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeJSON)); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.jsonKey(key)), jsonData); err != nil {
			return err
		}
		wrote = true
		return nil
	}, 30)
	if err != nil {
		return "", false, err
	}
	if !wrote {
		return "OK", false, nil
	}
	return "OK", true, nil
}

// JSONGet implements JSON.GET command
// JSON.GET key [path [path ...]]
func (s *BotreonStore) JSONGet(key string, paths ...string) ([]string, error) {
	// Default to root path
	if len(paths) == 0 {
		paths = []string{"$"}
	}

	jsonKey := s.jsonKey(key)
	var jsonData []byte

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
			return err
		}
		item, err := txn.Get([]byte(jsonKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		jsonData = val
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	// Parse JSON
	var root interface{}
	if err := json.Unmarshal(jsonData, &root); err != nil {
		return nil, errors.New("ERR invalid JSON data")
	}

	results := make([]string, 0, len(paths))
	for _, path := range paths {
		result, err := getValueByPath(root, path)
		if err != nil {
			results = append(results, "")
			continue
		}
		resultJSON, err := json.Marshal(result)
		if err != nil {
			results = append(results, "")
			continue
		}
		results = append(results, string(resultJSON))
	}

	return results, nil
}

// JSONDel implements JSON.DEL command
// JSON.DEL key [path ...]
func (s *BotreonStore) JSONDel(key string, paths ...string) (int64, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	// Default to root path (delete entire key)
	if len(paths) == 0 {
		paths = []string{"$"}
	}

	// If deleting root, delete the entire key
	if len(paths) == 1 && (paths[0] == "$" || paths[0] == ".") {
		var deleted int64
		err := s.retryUpdate(func(txn *badger.Txn) error {
			deleted = 0
			if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					return nil
				}
				return err
			}
			_, err := txn.Get([]byte(s.jsonKey(key)))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := txn.Delete(TypeOfKeyGet(key)); err != nil {
				return err
			}
			if err := txn.Delete([]byte(s.jsonKey(key))); err != nil {
				return err
			}
			deleted = 1
			return nil
		}, 30)
		if err != nil {
			return 0, err
		}
		return deleted, nil
	}

	// For now, we only support deleting the entire key
	// Partial path deletion would require more complex JSON manipulation
	return 0, nil
}

// JSONType implements JSON.TYPE command
// JSON.TYPE key [path]
func (s *BotreonStore) JSONType(key string, path string) (string, error) {
	if path == "" {
		path = "$"
	}

	jsonKey := s.jsonKey(key)
	var jsonData []byte

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
			return err
		}
		item, err := txn.Get([]byte(jsonKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		jsonData = val
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", ErrKeyNotFound
		}
		return "", err
	}

	// Parse JSON
	var root interface{}
	if err := json.Unmarshal(jsonData, &root); err != nil {
		return "", errors.New("ERR invalid JSON data")
	}

	// Get value at path
	value, err := getValueByPath(root, path)
	if err != nil {
		return "", nil
	}

	return getJSONType(value), nil
}

// JSONMGet implements JSON.MGET command
// JSON.MGET key [key ...] [path]
func (s *BotreonStore) JSONMGet(path string, keys ...string) ([]string, error) {
	results := make([]string, len(keys))
	for i, key := range keys {
		result, err := s.JSONGet(key, path)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				results[i] = ""
				continue
			}
			results[i] = ""
			continue
		}
		if len(result) > 0 {
			results[i] = result[0]
		} else {
			results[i] = ""
		}
	}
	return results, nil
}

// JSONArrAppend implements JSON.ARRAPPEND command
// JSON.ARRAPPEND key path value [value ...]
func (s *BotreonStore) JSONArrAppend(key, path string, values ...string) (int64, error) {
	if path != "$" && path != "." {
		return 0, errors.New("ERR path must be '$' or '.'")
	}

	var newLen int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		newLen = 0
		jsonData, err := s.jsonReadBytesInTxn(txn, key)
		if err != nil {
			return err
		}

		var rootPtr *interface{}
		if err := json.Unmarshal(jsonData, &rootPtr); err != nil {
			return errors.New("ERR invalid JSON data")
		}
		if rootPtr == nil {
			return errors.New("ERR invalid JSON data")
		}

		root := *rootPtr
		arr, ok := root.([]interface{})
		if !ok {
			return errors.New("ERR path does not resolve to an array")
		}

		for _, valStr := range values {
			var val interface{}
			if err := json.Unmarshal([]byte(valStr), &val); err != nil {
				val = valStr
			}
			arr = append(arr, val)
		}
		*rootPtr = arr

		newData, err := json.Marshal(rootPtr)
		if err != nil {
			return err
		}
		newLen = int64(len(arr))
		return txn.Set([]byte(s.jsonKey(key)), newData)
	}, 30)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}
	return newLen, nil
}

// JSONArrLen implements JSON.ARRLEN command
// JSON.ARRLEN key [path]
func (s *BotreonStore) JSONArrLen(key, path string) (int64, error) {
	if path == "" {
		path = "$"
	}

	jsonKey := s.jsonKey(key)
	var jsonData []byte

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
			return err
		}
		item, err := txn.Get([]byte(jsonKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		jsonData = val
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}

	// Parse JSON
	var root interface{}
	if err := json.Unmarshal(jsonData, &root); err != nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	// Get the array at path
	value, err := getValueByPath(root, path)
	if err != nil {
		return 0, nil
	}

	arr, ok := value.([]interface{})
	if !ok {
		return 0, errors.New("ERR path does not resolve to an array")
	}

	return int64(len(arr)), nil
}

// JSONObjKeys implements JSON.OBJKEYS command
// JSON.OBJKEYS key [path]
func (s *BotreonStore) JSONObjKeys(key, path string) ([]string, error) {
	if path == "" {
		path = "$"
	}

	jsonKey := s.jsonKey(key)
	var jsonData []byte

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
			return err
		}
		item, err := txn.Get([]byte(jsonKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		jsonData = val
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	// Parse JSON
	var root interface{}
	if err := json.Unmarshal(jsonData, &root); err != nil {
		return nil, errors.New("ERR invalid JSON data")
	}

	// Get the object at path
	value, err := getValueByPath(root, path)
	if err != nil {
		return nil, nil
	}

	obj, ok := value.(map[string]interface{})
	if !ok {
		return nil, errors.New("ERR path does not resolve to an object")
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	return keys, nil
}

// JSONNumIncrBy implements JSON.NUMINCRBY command
// JSON.NUMINCRBY key path increment
func (s *BotreonStore) JSONNumIncrBy(key, path string, increment float64) (float64, error) {
	if path != "$" && path != "." {
		return 0, errors.New("ERR path must be '$' or '.'")
	}

	var result float64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		result = 0
		jsonData, err := s.jsonReadBytesInTxn(txn, key)
		if err != nil {
			return err
		}

		var rootPtr *interface{}
		if err := json.Unmarshal(jsonData, &rootPtr); err != nil {
			return errors.New("ERR invalid JSON data")
		}
		if rootPtr == nil {
			return errors.New("ERR invalid JSON data")
		}

		root := *rootPtr
		value, err := getValueByPath(root, path)
		if err != nil {
			return err
		}

		num, ok := value.(float64)
		if !ok {
			if intVal, ok := value.(int); ok {
				num = float64(intVal)
			} else if int64Val, ok := value.(int64); ok {
				num = float64(int64Val)
			} else {
				return errors.New("ERR value at path is not a number")
			}
		}

		num += increment
		if m, ok := root.(map[string]interface{}); ok {
			m[""] = num
			*rootPtr = root
		} else if arr, ok := root.([]interface{}); ok {
			if len(arr) > 0 {
				arr[0] = num
			}
			*rootPtr = root
		} else {
			*rootPtr = num
		}

		newData, err := json.Marshal(rootPtr)
		if err != nil {
			return err
		}
		result = num
		return txn.Set([]byte(s.jsonKey(key)), newData)
	}, 30)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}
	return result, nil
}

// JSONNumMultBy implements JSON.NUMMULTBY command
// JSON.NUMMULTBY key path multiplier
func (s *BotreonStore) JSONNumMultBy(key, path string, multiplier float64) (float64, error) {
	if path != "$" && path != "." {
		return 0, errors.New("ERR path must be '$' or '.'")
	}

	var result float64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		result = 0
		jsonData, err := s.jsonReadBytesInTxn(txn, key)
		if err != nil {
			return err
		}

		var rootPtr *interface{}
		if err := json.Unmarshal(jsonData, &rootPtr); err != nil {
			return errors.New("ERR invalid JSON data")
		}
		if rootPtr == nil {
			return errors.New("ERR invalid JSON data")
		}

		root := *rootPtr
		value, err := getValueByPath(root, path)
		if err != nil {
			return err
		}

		num, ok := value.(float64)
		if !ok {
			if intVal, ok := value.(int); ok {
				num = float64(intVal)
			} else if int64Val, ok := value.(int64); ok {
				num = float64(int64Val)
			} else {
				return errors.New("ERR value at path is not a number")
			}
		}

		num *= multiplier
		if m, ok := root.(map[string]interface{}); ok {
			m[""] = num
			*rootPtr = root
		} else if arr, ok := root.([]interface{}); ok {
			if len(arr) > 0 {
				arr[0] = num
			}
			*rootPtr = root
		} else {
			*rootPtr = num
		}

		newData, err := json.Marshal(rootPtr)
		if err != nil {
			return err
		}
		result = num
		return txn.Set([]byte(s.jsonKey(key)), newData)
	}, 30)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}
	return result, nil
}

// JSONClear implements JSON.CLEAR command
// JSON.CLEAR key [path]
func (s *BotreonStore) JSONClear(key, path string) (int64, error) {
	if path == "" {
		path = "$"
	}

	var cleared int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		cleared = 0
		if path == "$" || path == "." {
			if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
				return err
			}
			_, err := txn.Get([]byte(s.jsonKey(key)))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			if err != nil {
				return err
			}
			cleared = 1
			return txn.Set([]byte(s.jsonKey(key)), []byte("{}"))
		}

		jsonData, err := s.jsonReadBytesInTxn(txn, key)
		if err != nil {
			return err
		}
		var root interface{}
		if err := json.Unmarshal(jsonData, &root); err != nil {
			return errors.New("ERR invalid JSON data")
		}
		newData, err := json.Marshal(root)
		if err != nil {
			return errors.New("ERR failed to marshal JSON")
		}
		return txn.Set([]byte(s.jsonKey(key)), newData)
	}, 30)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}
	return cleared, nil
}

// JSONDebug implements JSON.DEBUG command
// JSON.DEBUG MEMORY key [path]
func (s *BotreonStore) JSONDebugMemory(key, path string) (int64, error) {
	jsonKey := s.jsonKey(key)
	var jsonData []byte

	err := s.db.View(func(txn *badger.Txn) error {
		if err := checkKeyType(txn, key, KeyTypeJSON); err != nil {
			return err
		}
		item, err := txn.Get([]byte(jsonKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		jsonData = val
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, ErrKeyNotFound
		}
		return 0, err
	}

	// If path is root, return full JSON data size
	if path == "$" || path == "" {
		return int64(len(jsonData)), nil
	}

	// Parse JSON and extract value at path
	var root interface{}
	if err := json.Unmarshal(jsonData, &root); err != nil {
		return 0, fmt.Errorf("failed to parse JSON: %w", err)
	}

	val, err := getValueByPath(root, path)
	if err != nil {
		return 0, err
	}

	// Serialize the value back to JSON and measure its size
	serialized, err := json.Marshal(val)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize path value: %w", err)
	}

	return int64(len(serialized)), nil
}

// getValueByPath gets a value from JSON by path.
// 支持 RedisJSON 路径子集：字段访问（$.a.b）与数组下标（$[0]、$.arr[1]、
// $.arr[-1] 负索引），不支持通配符与过滤表达式。
func getValueByPath(root interface{}, path string) (interface{}, error) {
	// Normalize path: 去掉 $ 前缀，得到 token 流（字段名或 [index]）
	path = strings.TrimPrefix(path, "$")
	current := root

	// 手动解析 token：字段（.name）或数组下标（[n]）
	for i := 0; i < len(path); {
		switch path[i] {
		case '.':
			i++
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			field := path[start:i]
			if field == "" {
				continue
			}
			m, ok := current.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("path not traversable: %s", field)
			}
			val, ok := m[field]
			if !ok {
				return nil, fmt.Errorf("path not found: %s", field)
			}
			current = val
		case '[':
			end := strings.IndexByte(path[i:], ']')
			if end == -1 {
				return nil, fmt.Errorf("invalid path: unmatched '['")
			}
			idxStr := path[i+1 : i+end]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", idxStr)
			}
			arr, ok := current.([]interface{})
			if !ok {
				return nil, fmt.Errorf("path not traversable: [%s]", idxStr)
			}
			if idx < 0 {
				idx += len(arr) // 负索引从末尾数
			}
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("array index out of range: %s", idxStr)
			}
			current = arr[idx]
			i += end + 1
		default:
			// 兼容无 $ 前缀且无前导点的路径（如 "a.b"）
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			field := path[start:i]
			m, ok := current.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("path not traversable: %s", field)
			}
			val, ok := m[field]
			if !ok {
				return nil, fmt.Errorf("path not found: %s", field)
			}
			current = val
		}
	}

	return current, nil
}

// getJSONType returns the type of a JSON value
func getJSONType(value interface{}) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case float64:
		return "number"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}
