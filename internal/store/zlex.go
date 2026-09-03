package store

import (
	"github.com/dgraph-io/badger/v4"
)

// memberInLexRange checks if a member is within a lexicographic range.
func memberInLexRange(memberStr, min, max string) bool {
	minOK := compareLex(min, memberStr, true)
	var maxOK bool
	switch {
	case max == "+":
		maxOK = true
	case len(max) > 0 && max[0] == '(':
		maxOK = memberStr < max[1:]
	case len(max) > 0 && max[0] == '[':
		maxOK = memberStr <= max[1:]
	default:
		maxOK = memberStr <= max
	}
	return minOK && maxOK
}

// compareLex 比较两个字符串（字典序），支持开区间和闭区间
func compareLex(a, b string, inclusive bool) bool {
	if a == "-" {
		return true
	}
	if a == "+" {
		return false
	}
	if b == "-" {
		return false
	}
	if b == "+" {
		return true
	}

	var aVal string
	var aIncl bool
	if len(a) > 0 {
		switch a[0] {
		case '(':
			aVal = a[1:]
			aIncl = false
		case '[':
			aVal = a[1:]
			aIncl = true
		default:
			aVal = a
			aIncl = inclusive
		}
	} else {
		aVal = a
		aIncl = inclusive
	}

	var bVal string
	if len(b) > 0 {
		if b[0] == '(' || b[0] == '[' {
			bVal = b[1:]
		} else {
			bVal = b
		}
	} else {
		bVal = b
	}

	var useIncl bool
	if len(a) > 0 && (a[0] == '(' || a[0] == '[') {
		useIncl = aIncl
	} else {
		if len(b) > 0 && (b[0] == '(' || b[0] == '[') {
			useIncl = b[0] == '['
		} else {
			useIncl = inclusive
		}
	}

	if useIncl {
		return aVal <= bVal
	}
	return aVal < bVal
}

// zFilterMemberNamesByLex filters members by a lexicographic range.
func zFilterMemberNamesByLex(members []ZSetMember, min, max string) []string {
	var filtered []string
	for _, member := range members {
		if memberInLexRange(member.Member, min, max) {
			filtered = append(filtered, member.Member)
		}
	}
	return filtered
}

// ZLexCount 实现 Redis ZLEXCOUNT 命令
func (s *BotreonStore) ZLexCount(zSetName, min, max string) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := zReadZSetMembersInTxn(txn, zSetName)
		if err != nil {
			return err
		}
		for _, member := range members {
			if compareLex(min, member.Member, true) && compareLex(member.Member, max, false) {
				count++
			}
		}
		return nil
	})
	return count, err
}

// ZRangeByLex 实现 Redis ZRANGEBYLEX 命令
func (s *BotreonStore) ZRangeByLex(zSetName, min, max string, offset, count int) ([]string, error) {
	var filtered []string
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := zReadZSetMembersInTxn(txn, zSetName)
		if err != nil {
			return err
		}
		filtered = zFilterMemberNamesByLex(members, min, max)
		return nil
	})
	if err != nil {
		return nil, err
	}

	var results []string
	if offset < 0 {
		offset = 0
	}
	if offset < len(filtered) {
		end := offset + count
		if count <= 0 || end > len(filtered) {
			end = len(filtered)
		}
		results = filtered[offset:end]
	}
	return results, nil
}

// ZRevRangeByLex 实现 Redis ZREVRANGEBYLEX 命令
func (s *BotreonStore) ZRevRangeByLex(zSetName, max, min string, offset, count int) ([]string, error) {
	var filtered []string
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := zReadZSetMembersInTxn(txn, zSetName)
		if err != nil {
			return err
		}
		filtered = zFilterMemberNamesByLex(members, min, max)
		return nil
	})
	if err != nil {
		return nil, err
	}

	for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
		filtered[i], filtered[j] = filtered[j], filtered[i]
	}

	var results []string
	if offset < 0 {
		offset = 0
	}
	if offset < len(filtered) {
		end := offset + count
		if count <= 0 || end > len(filtered) {
			end = len(filtered)
		}
		results = filtered[offset:end]
	}
	return results, nil
}

// ZRemRangeByLex 实现 Redis ZREMRANGEBYLEX 命令
func (s *BotreonStore) ZRemRangeByLex(zSetName, min, max string) (int64, error) {
	s.keyLockMgr.Lock(zSetName)
	defer s.keyLockMgr.Unlock(zSetName)
	var removed int64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		removed = 0
		members, err := zRangeAllMembersInTxn(txn, zSetName)
		if err != nil {
			return err
		}

		for _, member := range members {
			if !memberInLexRange(member.Member, min, max) {
				continue
			}
			n, err := zRemMemberInTxn(txn, zSetName, member.Member)
			if err != nil {
				return err
			}
			removed += n
		}
		return nil
	}, 20)
	s.markZSetDirty(zSetName)
	return removed, err
}
