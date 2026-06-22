package backup

import (
	"context"
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/store"
)

// BackupManager 统一的备份管理器
type BackupManager struct {
	store          *store.BotreonStore
	badgerMgr      *BadgerBackupManager
	rdbMgr         *RDBBackupManager
	lastSaveTime   int64
	lastSaveTimeMu sync.RWMutex
	backupDir      string

	wg sync.WaitGroup
}

// NewBackupManager 创建新的备份管理器
func NewBackupManager(store *store.BotreonStore, backupDir string) *BackupManager {
	return &BackupManager{
		store:     store,
		badgerMgr: NewBadgerBackupManager(store.GetDB()),
		rdbMgr:    NewRDBBackupManager(store),
		backupDir: backupDir,
	}
}

// Save 同步保存RDB
func (bm *BackupManager) Save() error {
	backupFile, err := bm.rdbMgr.Backup(bm.backupDir)
	if err != nil {
		return err
	}

	bm.lastSaveTimeMu.Lock()
	bm.lastSaveTime = time.Now().Unix()
	bm.lastSaveTimeMu.Unlock()

	logger.Logger.Info().Str("file", backupFile).Msg("RDB backup saved")
	return nil
}

// BGSave 后台保存RDB。
// ctx 控制后台保存的生命周期；当 ctx 被取消时（如服务器关闭），
// BGSave 会立即返回，不等待 Save 完成，避免 shutdown 被阻塞。
// 调用者应在 ctx 取消后调用 Wait()，Wait() 会在所有监控 goroutine
// 退出后返回（而非等 Save 实际完成）。
func (bm *BackupManager) BGSave(ctx context.Context) error {
	bm.wg.Add(1)
	go func() {
		defer bm.wg.Done()

		done := make(chan struct{})
		go func() {
			if err := bm.Save(); err != nil {
				logger.Logger.Error().Err(err).Msg("BGSAVE failed")
			}
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			logger.Logger.Warn().Msg("BGSAVE cancelled during shutdown")
		}
	}()
	return nil
}

// Wait 等待所有后台备份完成
func (bm *BackupManager) Wait() {
	bm.wg.Wait()
}

// LastSave 获取最后保存时间
func (bm *BackupManager) LastSave() int64 {
	bm.lastSaveTimeMu.RLock()
	defer bm.lastSaveTimeMu.RUnlock()
	return bm.lastSaveTime
}

// BackupBadger 执行BadgerDB备份
func (bm *BackupManager) BackupBadger() (string, error) {
	return bm.badgerMgr.Backup(bm.backupDir)
}

// BackupRDB 执行RDB备份
func (bm *BackupManager) BackupRDB() (string, error) {
	return bm.rdbMgr.Backup(bm.backupDir)
}

// BackupBoth 同时执行两种格式的备份
func (bm *BackupManager) BackupBoth() ([]string, error) {
	var files []string

	// BadgerDB备份
	badgerFile, err := bm.badgerMgr.Backup(bm.backupDir)
	if err != nil {
		return nil, err
	}
	files = append(files, badgerFile)

	// RDB备份
	rdbFile, err := bm.rdbMgr.Backup(bm.backupDir)
	if err != nil {
		return nil, err
	}
	files = append(files, rdbFile)

	return files, nil
}
