package store

import (
	"encoding/json"
	"errors"
	"fmt"
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

// JSONSet implements JSON.SET command
// JSON.SET key path value [NX | XX]
func (s *BotreonStore) JSONSet(key, path, value string, nx, xx bool) (string, error) {
	// Only support root path for now
	if path != "$" && path != "." {
		return "", errors.New("ERR path must be '$' or '.'")
	}

	// Validate JSON first
	var newValue interface{}
	if err := json.Unmarshal([]byte(value), &newValue); err != nil {
		return "", errors.New("ERR invalid JSON")
	}

	// Check if key exists
	var exists bool
	err := s.db.View(func(txn *badger.Txn) error {
		jsonKey := []byte(s.jsonKey(key))
		_, err := txn.Get(jsonKey)
		if err == nil {
			exists = true
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	// Handle NX/XX options
	if nx && exists {
		return "OK", nil // NX: do not update if exists
	}
	if xx && !exists {
		return "OK", nil // XX: do not create if not exists
	}

	// Store the JSON
	jsonData, err := json.Marshal(newValue)
	if err != nil {
		return "", err
	}

	// Clear read cache
	if s.readCache != nil {
		s.readCache.Delete(key)
	}

	err = s.retryUpdate(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
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
		// Set type
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeJSON)); err != nil {
			return err
		}
		// Set JSON value
		return txn.Set([]byte(s.jsonKey(key)), jsonData)
	}, 30)

	if err != nil {
		return "", err
	}
	return "OK", nil
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
	// Default to root path (delete entire key)
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
			return 0, nil
		}
		return 0, err
	}

	// Parse JSON
	var root interface{}
	if err := json.Unmarshal(jsonData, &root); err != nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	deleted := int64(0)

	// If deleting root, delete the entire key
	if len(paths) == 1 && (paths[0] == "$" || paths[0] == ".") {
		// Clear read cache
		if s.readCache != nil {
			s.readCache.Delete(key)
		}

		err = s.retryUpdate(func(txn *badger.Txn) error {
			if err := txn.Delete(TypeOfKeyGet(key)); err != nil {
				return err
			}
			return txn.Delete([]byte(jsonKey))
		}, 30)
		if err != nil {
			return 0, err
		}
		return 1, nil
	}

	// For now, we only support deleting the entire key
	// Partial path deletion would require more complex JSON manipulation
	return deleted, nil
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

	// Parse JSON into a pointer to interface{} so modifications persist
	var rootPtr *interface{}
	if err := json.Unmarshal(jsonData, &rootPtr); err != nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	if rootPtr == nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	root := *rootPtr

	// Get the array at root
	arr, ok := root.([]interface{})
	if !ok {
		return 0, errors.New("ERR path does not resolve to an array")
	}

	// Parse and append new values
	for _, valStr := range values {
		var val interface{}
		if err := json.Unmarshal([]byte(valStr), &val); err != nil {
			// Treat as string if not valid JSON
			val = valStr
		}
		// Append to the slice
		arr = append(arr, val)
	}

	// Update the root pointer with the modified array
	*rootPtr = arr

	// Marshal back
	newData, err := json.Marshal(rootPtr)
	if err != nil {
		return 0, err
	}

	// Clear read cache
	if s.readCache != nil {
		s.readCache.Delete(key)
	}

	err = s.retryUpdate(func(txn *badger.Txn) error {
		return txn.Set([]byte(jsonKey), newData)
	}, 30)
	if err != nil {
		return 0, err
	}

	return int64(len(arr)), nil
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

	// Parse JSON into a pointer to interface{} so modifications persist
	var rootPtr *interface{}
	if err := json.Unmarshal(jsonData, &rootPtr); err != nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	if rootPtr == nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	root := *rootPtr

	// Get the value at root
	value, err := getValueByPath(root, path)
	if err != nil {
		return 0, err
	}

	// Check if it's a number
	num, ok := value.(float64)
	if !ok {
		// Try int
		if intVal, ok := value.(int); ok {
			num = float64(intVal)
		} else if int64Val, ok := value.(int64); ok {
			num = float64(int64Val)
		} else {
			return 0, errors.New("ERR value at path is not a number")
		}
	}

	// Increment
	num += increment

	// Update root
	if m, ok := root.(map[string]interface{}); ok {
		m[""] = num
	} else if arr, ok := root.([]interface{}); ok {
		if len(arr) > 0 {
			arr[0] = num
		}
	}

	// Update the root pointer
	*rootPtr = root

	// Marshal back
	newData, err := json.Marshal(rootPtr)
	if err != nil {
		return 0, err
	}

	// Clear read cache
	if s.readCache != nil {
		s.readCache.Delete(key)
	}

	err = s.retryUpdate(func(txn *badger.Txn) error {
		return txn.Set([]byte(jsonKey), newData)
	}, 30)
	if err != nil {
		return 0, err
	}

	return num, nil
}

// JSONNumMultBy implements JSON.NUMMULTBY command
// JSON.NUMMULTBY key path multiplier
func (s *BotreonStore) JSONNumMultBy(key, path string, multiplier float64) (float64, error) {
	if path != "$" && path != "." {
		return 0, errors.New("ERR path must be '$' or '.'")
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

	// Parse JSON into a pointer to interface{} so modifications persist
	var rootPtr *interface{}
	if err := json.Unmarshal(jsonData, &rootPtr); err != nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	if rootPtr == nil {
		return 0, errors.New("ERR invalid JSON data")
	}

	root := *rootPtr

	// Get the value at root
	value, err := getValueByPath(root, path)
	if err != nil {
		return 0, err
	}

	// Check if it's a number
	num, ok := value.(float64)
	if !ok {
		// Try int
		if intVal, ok := value.(int); ok {
			num = float64(intVal)
		} else if int64Val, ok := value.(int64); ok {
			num = float64(int64Val)
		} else {
			return 0, errors.New("ERR value at path is not a number")
		}
	}

	// Multiply
	num = num * multiplier

	// Update root
	if m, ok := root.(map[string]interface{}); ok {
		m[""] = num
	} else if arr, ok := root.([]interface{}); ok {
		if len(arr) > 0 {
			arr[0] = num
		}
	}

	// Update the root pointer
	*rootPtr = root

	// Marshal back
	newData, err := json.Marshal(rootPtr)
	if err != nil {
		return 0, err
	}

	// Clear read cache
	if s.readCache != nil {
		s.readCache.Delete(key)
	}

	err = s.retryUpdate(func(txn *badger.Txn) error {
		return txn.Set([]byte(jsonKey), newData)
	}, 30)
	if err != nil {
		return 0, err
	}

	return num, nil
}

// JSONClear implements JSON.CLEAR command
// JSON.CLEAR key [path]
func (s *BotreonStore) JSONClear(key, path string) (int64, error) {
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

	// For now, clear means empty array or object
	cleared := int64(0)

	// Marshal back
	var newData []byte
	if path == "$" || path == "." {
		newData = []byte("{}")
		cleared = 1
	} else {
		newData, _ = json.Marshal(root)
	}

	// Clear read cache
	if s.readCache != nil {
		s.readCache.Delete(key)
	}

	err = s.retryUpdate(func(txn *badger.Txn) error {
		return txn.Set([]byte(jsonKey), newData)
	}, 30)
	if err != nil {
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

// getValueByPath gets a value from JSON by path (simplified JSONPath)
func getValueByPath(root interface{}, path string) (interface{}, error) {
	// Normalize path
	path = strings.TrimPrefix(path, "$")
	if len(path) > 0 && path[0] == '.' {
		path = path[1:]
	}
	if path == "" {
		return root, nil
	}

	// Split path by dot
	parts := strings.Split(path, ".")
	current := root

	for _, part := range parts {
		if part == "" {
			continue
		}

		switch v := current.(type) {
		case map[string]interface{}:
			val, ok := v[part]
			if !ok {
				return nil, fmt.Errorf("path not found: %s", part)
			}
			current = val
		default:
			return nil, fmt.Errorf("path not traversable: %s", part)
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
