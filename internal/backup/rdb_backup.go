package backup

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// RDBBackupManager RDB格式备份管理器
type RDBBackupManager struct {
	store *store.BotreonStore
}

// NewRDBBackupManager 创建新的RDB备份管理器
func NewRDBBackupManager(store *store.BotreonStore) *RDBBackupManager {
	return &RDBBackupManager{
		store: store,
	}
}

// Backup 执行RDB备份
func (rbm *RDBBackupManager) Backup(backupDir string) (string, error) {
	// 创建备份目录
	if err := os.MkdirAll(backupDir, 0750); err != nil {
		return "", fmt.Errorf("create backup directory failed: %w", err)
	}

	// 生成备份文件名
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(backupDir, fmt.Sprintf("dump_%s.rdb", timestamp))

	// 生成RDB数据
	rdbData, err := replication.GenerateRDB(rbm.store)
	if err != nil {
		return "", fmt.Errorf("generate RDB failed: %w", err)
	}

	// 写入文件
	if err := os.WriteFile(backupFile, rdbData, 0600); err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("write RDB file failed")
		return "", fmt.Errorf("write RDB file failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Int("size", len(rdbData)).
		Msg("RDB备份完成")

	return backupFile, nil
}

// BackupWithCompression 执行压缩RDB备份
func (rbm *RDBBackupManager) BackupWithCompression(backupDir string) (string, error) {
	// 创建备份目录
	if err := os.MkdirAll(backupDir, 0750); err != nil {
		return "", fmt.Errorf("create backup directory failed: %w", err)
	}

	// 生成备份文件名
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(backupDir, fmt.Sprintf("dump_%s.rdb.gz", timestamp))

	// 生成RDB数据
	rdbData, err := replication.GenerateRDB(rbm.store)
	if err != nil {
		return "", fmt.Errorf("generate RDB failed: %w", err)
	}

	// 创建目标文件
	f, err := os.OpenFile(filepath.Clean(backupFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return "", fmt.Errorf("create backup file failed: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("failed to close backup file")
		}
	}()

	// 使用 gzip 压缩写入
	gzWriter, err := gzip.NewWriterLevel(f, gzip.DefaultCompression)
	if err != nil {
		return "", fmt.Errorf("create gzip writer failed: %w", err)
	}

	if _, err := gzWriter.Write(rdbData); err != nil {
		return "", fmt.Errorf("write compressed data failed: %w", err)
	}

	if err := gzWriter.Close(); err != nil {
		return "", fmt.Errorf("close gzip writer failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Int("original_size", len(rdbData)).
		Msg("压缩RDB备份完成")

	return backupFile, nil
}

// GetBackupInfo 获取备份信息
func (rbm *RDBBackupManager) GetBackupInfo(backupFile string) (map[string]interface{}, error) {
	backupFile = filepath.Clean(backupFile)
	file, err := os.Open(backupFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("open backup file failed")
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
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("get file info failed")
		return nil, fmt.Errorf("get file info failed: %w", err)
	}

	info["size"] = fileInfo.Size()
	info["mod_time"] = fileInfo.ModTime()
	info["path"] = backupFile
	info["format"] = "RDB"

	return info, nil
}

// ListRDBBackups 列出RDB备份（包括 .rdb 和 .rdb.gz 文件）
func ListRDBBackups(backupDir string) ([]string, error) {
	files, err := os.ReadDir(backupDir)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_dir", backupDir).Msg("read backup directory failed")
		return nil, fmt.Errorf("read backup directory failed: %w", err)
	}

	var backups []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		ext := filepath.Ext(file.Name())
		if ext == ".rdb" || ext == ".gz" && filepath.Ext(file.Name()[:len(file.Name())-3]) == ".rdb" {
			backups = append(backups, filepath.Join(backupDir, file.Name()))
		}
	}

	return backups, nil
}
