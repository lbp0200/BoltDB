package backup

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/store"
)

// RestoreManager 备份恢复管理器
type RestoreManager struct {
	store *store.BotreonStore
}

// NewRestoreManager 创建新的恢复管理器
func NewRestoreManager(store *store.BotreonStore) *RestoreManager {
	return &RestoreManager{
		store: store,
	}
}

// RestoreFromBadger 从BadgerDB备份恢复
func (rm *RestoreManager) RestoreFromBadger(backupFile string) error {
	badgerMgr := NewBadgerBackupManager(rm.store.GetDB())
	return badgerMgr.Restore(backupFile)
}

// RestoreFromRDB 从RDB文件恢复
func (rm *RestoreManager) RestoreFromRDB(rdbFile string) error {
	rdbFile = filepath.Clean(rdbFile)
	logger.Logger.Info().Str("rdb_file", rdbFile).Msg("开始从RDB文件恢复")

	// 检测是否为 gzip 压缩文件
	var rdbData []byte
	file, err := os.Open(rdbFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("rdb_file", rdbFile).Msg("打开RDB文件失败")
		return fmt.Errorf("open RDB file failed: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("rdb_file", rdbFile).Msg("关闭RDB文件失败")
		}
	}()

	// 读取前两个字节判断是否为 gzip 格式
	header := make([]byte, 2)
	if _, err := file.Read(header); err != nil {
		_ = file.Close()
		return fmt.Errorf("read RDB file header failed: %w", err)
	}
	_ = file.Close()

	if header[0] == 0x1F && header[1] == 0x8B {
		// gzip 压缩格式
		logger.Logger.Info().Msg("检测到gzip压缩格式，先解压")
		rdbData, err = rm.readCompressedRDB(rdbFile)
	} else {
		// 普通 RDB 格式
		rdbData, err = os.ReadFile(rdbFile)
	}
	if err != nil {
		return err
	}

	logger.Logger.Info().Int("size", len(rdbData)).Msg("RDB文件读取成功，开始解析")

	// 使用 replication.LoadRDBWithStore 解析并加载RDB数据
	if err := replication.LoadRDBWithStore(rdbData, rm.store); err != nil {
		logger.Logger.Error().Err(err).Msg("RDB恢复失败")
		return fmt.Errorf("RDB restore failed: %w", err)
	}

	logger.Logger.Info().Msg("RDB恢复完成")
	return nil
}

// readCompressedRDB 读取压缩的RDB文件
func (rm *RestoreManager) readCompressedRDB(rdbFile string) ([]byte, error) {
	f, err := os.Open(filepath.Clean(rdbFile))
	if err != nil {
		return nil, fmt.Errorf("open compressed RDB file failed: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Logger.Error().Err(err).Str("rdb_file", rdbFile).Msg("关闭压缩RDB文件失败")
		}
	}()

	gzReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("create gzip reader failed: %w", err)
	}
	defer func() {
		if err := gzReader.Close(); err != nil {
			logger.Logger.Error().Err(err).Msg("关闭gzip读取器失败")
		}
	}()

	// 读取所有解压后的数据
	var decompressed []byte
	buf := make([]byte, 32*1024) // 32KB buffer
	for {
		n, err := gzReader.Read(buf)
		if n > 0 {
			decompressed = append(decompressed, buf[:n]...)
		}
		if err != nil {
			break
		}
	}

	logger.Logger.Info().Int("decompressed_size", len(decompressed)).Msg("gzip解压完成")
	return decompressed, nil
}

// RestoreFromPath 从路径恢复（自动检测格式）
func (rm *RestoreManager) RestoreFromPath(backupPath string) error {
	// 检查文件是否存在
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		return fmt.Errorf("backup file not found: %s", backupPath)
	}

	name := filepath.Base(backupPath)
	ext := filepath.Ext(name)

	// 处理 .rdb.gz 后缀
	if ext == ".gz" && len(name) > 3 {
		innerExt := filepath.Ext(name[:len(name)-3])
		if innerExt == ".rdb" {
			return rm.RestoreFromRDB(backupPath)
		}
	}

	switch ext {
	case ".rdb":
		return rm.RestoreFromRDB(backupPath)
	case "", ".bak":
		// BadgerDB备份通常没有扩展名或使用.bak
		return rm.RestoreFromBadger(backupPath)
	default:
		return fmt.Errorf("unknown backup format: %s", ext)
	}
}
