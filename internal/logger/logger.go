package logger

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	// Logger 全局日志实例
	Logger zerolog.Logger
)

func init() {
	// 从环境变量读取日志配置
	logFile := os.Getenv("BOLTREON_LOG_FILE")
	levelStr := os.Getenv("BOLTREON_LOG_LEVEL")
	if levelStr == "" {
		levelStr = "warn" // 默认 WARNING 级别
	}

	level := parseLevel(levelStr)
	zerolog.SetGlobalLevel(level)

	var output interface {
		Write(p []byte) (n int, err error)
	}

	if logFile != "" {
		// 异步日志文件，带轮转
		output = &lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    100, // MB
			MaxBackups: 7,   // 保留7个备份
			MaxAge:     30,  // 天
			Compress:   true,
		}
	} else {
		// 控制台输出（带颜色）
		consoleWriter := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05.000",
		}
		output = consoleWriter
	}

	// 创建全局 logger
	Logger = zerolog.New(output).With().Timestamp().Logger()

	// 设置全局 logger
	log.Logger = Logger
}

// parseLevel 解析日志级别字符串
func parseLevel(levelStr string) zerolog.Level {
	levelStr = strings.ToUpper(strings.TrimSpace(levelStr))
	switch levelStr {
	case "DEBUG", "DBG":
		return zerolog.DebugLevel
	case "INFO", "INF":
		return zerolog.InfoLevel
	case "WARNING", "WARN":
		return zerolog.WarnLevel
	case "ERROR", "ERR":
		return zerolog.ErrorLevel
	case "FATAL":
		return zerolog.FatalLevel
	case "PANIC":
		return zerolog.PanicLevel
	case "TRACE":
		return zerolog.TraceLevel
	default:
		return zerolog.WarnLevel // 默认 WARNING
	}
}

// SetLevel 设置日志级别
func SetLevel(level zerolog.Level) {
	zerolog.SetGlobalLevel(level)
	Logger = Logger.Level(level)
	log.Logger = Logger
}

// SetLevelFromString 从字符串设置日志级别
func SetLevelFromString(levelStr string) {
	level := parseLevel(levelStr)
	SetLevel(level)
}

// GetLevel 获取当前日志级别
func GetLevel() zerolog.Level {
	return zerolog.GlobalLevel()
}

// GetLevelString 获取当前日志级别的字符串表示
func GetLevelString() string {
	return zerolog.GlobalLevel().String()
}

// Debug 记录 DEBUG 级别日志
func Debug(format string, args ...interface{}) {
	Logger.Debug().Msgf(format, args...)
}

// Info 记录 INFO 级别日志
func Info(format string, args ...interface{}) {
	Logger.Info().Msgf(format, args...)
}

// Warning 记录 WARNING 级别日志
func Warning(format string, args ...interface{}) {
	Logger.Warn().Msgf(format, args...)
}

// Error 记录 ERROR 级别日志
func Error(format string, args ...interface{}) {
	Logger.Error().Msgf(format, args...)
}

// DebugWith 记录带字段的 DEBUG 级别日志
func DebugWith(key string, value interface{}) *zerolog.Event {
	return Logger.Debug().Interface(key, value)
}

// InfoWith 记录带字段的 INFO 级别日志
func InfoWith(key string, value interface{}) *zerolog.Event {
	return Logger.Info().Interface(key, value)
}

// WarningWith 记录带字段的 WARNING 级别日志
func WarningWith(key string, value interface{}) *zerolog.Event {
	return Logger.Warn().Interface(key, value)
}

// ErrorWith 记录带字段的 ERROR 级别日志
func ErrorWith(key string, value interface{}) *zerolog.Event {
	return Logger.Error().Interface(key, value)
}
