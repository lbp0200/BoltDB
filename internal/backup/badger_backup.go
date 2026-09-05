package backup

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// BadgerBackupManager BadgerDB备份管理器
type BadgerBackupManager struct {
	db *badger.DB
}

// NewBadgerBackupManager 创建新的BadgerDB备份管理器
func NewBadgerBackupManager(db *badger.DB) *BadgerBackupManager {
	return &BadgerBackupManager{
		db: db,
	}
}

// Backup 执行BadgerDB备份
func (bbm *BadgerBackupManager) Backup(backupDir string) (string, error) {
	// 创建备份目录
	if err := os.MkdirAll(backupDir, 0750); err != nil {
		return "", fmt.Errorf("create backup directory failed: %w", err)
	}

	// 生成备份文件名
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(backupDir, fmt.Sprintf("badger_backup_%s", timestamp))

	// 验证路径不包含遍历符
	cleanFile := filepath.Clean(backupFile)
	if !strings.HasPrefix(cleanFile, filepath.Clean(backupDir)) {
		return "", fmt.Errorf("invalid backup path: path traversal detected")
	}

	// 创建备份文件
	file, err := os.Create(cleanFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", cleanFile).Msg("create backup file failed")
		return "", fmt.Errorf("create backup file failed: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("failed to close backup file")
		}
	}()

	// 执行备份（managed DB 兼容：badger 原生 Backup 内部 NewStream 在 managed
	// 模式 panic（v4.9.x stream.go）——改用 View 只读事务遍历 + len-prefixed
	// 全量 key/value 快照——恢复端（Restore/RestoreTo）配套读取）
	if err := bbm.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		w := bufio.NewWriter(file)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := binary.Write(w, binary.LittleEndian, uint32(len(key))); err != nil {
				return err
			}
			if _, err := w.Write(key); err != nil {
				return err
			}
			if err := binary.Write(w, binary.LittleEndian, uint32(len(val))); err != nil {
				return err
			}
			if _, err := w.Write(val); err != nil {
				return err
			}
		}
		return w.Flush()
	}); err != nil {
		return "", fmt.Errorf("backup failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Msg("BadgerDB备份完成")

	return backupFile, nil
}

// IncrementalBackup 执行增量备份
func (bbm *BadgerBackupManager) IncrementalBackup(backupDir string, since uint64) (string, error) {
	// 创建备份目录
	if err := os.MkdirAll(backupDir, 0750); err != nil {
		return "", fmt.Errorf("create backup directory failed: %w", err)
	}

	// 生成备份文件名
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(backupDir, fmt.Sprintf("badger_backup_inc_%s", timestamp))

	// 验证路径不包含遍历符
	cleanFile := filepath.Clean(backupFile)
	if !strings.HasPrefix(cleanFile, filepath.Clean(backupDir)) {
		return "", fmt.Errorf("invalid backup path: path traversal detected")
	}

	// 创建备份文件
	file, err := os.Create(cleanFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", cleanFile).Msg("create backup file failed")
		return "", fmt.Errorf("create backup file failed: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("failed to close backup file")
		}
	}()

	// 执行增量备份
	_, err = bbm.db.Backup(file, since)
	if err != nil {
		return "", fmt.Errorf("incremental backup failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Uint64("since", since).
		Msg("BadgerDB增量备份完成")

	return backupFile, nil
}

// Restore 从备份恢复
func (bbm *BadgerBackupManager) Restore(backupFile string) error {
	// 验证备份文件路径
	backupFile = filepath.Clean(backupFile)

	// 打开备份文件
	file, err := os.Open(backupFile)
	if err != nil {
		return fmt.Errorf("open backup file failed: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("failed to close backup file")
		}
	}()

	// 执行恢复（配套自实现备份格式：len-prefixed key/value 读取 + Update 写入）
	if err := bbm.db.Update(func(txn *badger.Txn) error {
		r := bufio.NewReader(file)
		for {
			var keyLen uint32
			if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			key := make([]byte, keyLen)
			if _, err := io.ReadFull(r, key); err != nil {
				return err
			}
			var valLen uint32
			if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
				return err
			}
			val := make([]byte, valLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return err
			}
			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
	}); err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("restore failed")
		return fmt.Errorf("restore failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Msg("BadgerDB恢复完成")

	return nil
}

// RestoreTo 恢复到新的数据库
func RestoreTo(backupFile, dbPath string) error {
	// 验证备份文件路径
	backupFile = filepath.Clean(backupFile)

	// 打开目标数据库
	opts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(opts)
	if err != nil {
		logger.Logger.Error().Err(err).Str("db_path", dbPath).Msg("open target database failed")
		return fmt.Errorf("open target database failed: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("db_path", dbPath).Msg("failed to close database")
		}
	}()

	// 打开备份文件
	file, err := os.Open(backupFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("open backup file failed")
		return fmt.Errorf("open backup file failed: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("failed to close backup file")
		}
	}()

	// 执行恢复（配套自实现备份格式：len-prefixed key/value 读取 + Update 写入）
	err = db.Update(func(txn *badger.Txn) error {
		r := bufio.NewReader(file)
		for {
			var keyLen uint32
			if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			key := make([]byte, keyLen)
			if _, err := io.ReadFull(r, key); err != nil {
				return err
			}
			var valLen uint32
			if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
				return err
			}
			val := make([]byte, valLen)
			if _, err := io.ReadFull(r, val); err != nil {
				return err
			}
			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
	})
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Str("db_path", dbPath).Msg("restore failed")
		return fmt.Errorf("restore failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Str("db_path", dbPath).
		Msg("BadgerDB恢复到新数据库完成")

	return nil
}

// GetBackupInfo 获取备份信息
func GetBackupInfo(backupFile string) (map[string]interface{}, error) {
	backupFile = filepath.Clean(backupFile)
	file, err := os.Open(backupFile)
	if err != nil {
		return nil, fmt.Errorf("open backup file failed: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("failed to close backup file")
		}
	}()

	info := make(map[string]interface{})

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("get file info failed: %w", err)
	}

	info["size"] = fileInfo.Size()
	info["mod_time"] = fileInfo.ModTime()
	info["path"] = backupFile

	return info, nil
}

// ListBackups 列出备份目录中的所有备份
func ListBackups(backupDir string) ([]string, error) {
	files, err := os.ReadDir(backupDir)
	if err != nil {
		return nil, fmt.Errorf("read backup directory failed: %w", err)
	}

	var backups []string
	for _, file := range files {
		if !file.IsDir() && (filepath.Ext(file.Name()) == "" || filepath.Ext(file.Name()) == ".bak") {
			backups = append(backups, filepath.Join(backupDir, file.Name()))
		}
	}

	return backups, nil
}

// CopyBackup 复制备份文件
func CopyBackup(src, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file failed: %w", err)
	}
	defer func() {
		if err := srcFile.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("src", src).Msg("failed to close source file")
		}
	}()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination file failed: %w", err)
	}
	defer func() {
		if err := dstFile.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("dst", dst).Msg("failed to close destination file")
		}
	}()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("copy file failed: %w", err)
	}

	return nil
}
