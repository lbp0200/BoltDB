package store

import (
	"sync"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// PubSubManager Pub/Sub管理器
type PubSubManager struct {
	mu            sync.RWMutex
	channels      map[string]map[*Subscriber]bool // 频道 -> 订阅者映射
	shardChannels map[string]map[*Subscriber]bool // shard 频道 -> 订阅者映射（SSUBSCRIBE）
	patterns      map[string]map[*Subscriber]bool // 模式 -> 订阅者映射
	subscribers   map[*Subscriber]bool            // 所有订阅者
}

// Subscriber 订阅者
type Subscriber struct {
	ID            string
	Channels      map[string]bool
	ShardChannels map[string]bool
	Patterns      map[string]bool
	MessageCh     chan *Message
	mu            sync.RWMutex
	closed        bool
	closeMu       sync.Mutex
}

// Message 消息
type Message struct {
	Channel string
	Pattern string
	Data    []byte
	Shard   bool // true = shard channel 消息（SSUBSCRIBE/SPUBLISH）
}

// NewPubSubManager 创建新的Pub/Sub管理器
func NewPubSubManager() *PubSubManager {
	return &PubSubManager{
		channels:      make(map[string]map[*Subscriber]bool),
		shardChannels: make(map[string]map[*Subscriber]bool),
		patterns:      make(map[string]map[*Subscriber]bool),
		subscribers:   make(map[*Subscriber]bool),
	}
}

// NewSubscriber 创建新的订阅者
func NewSubscriber(id string) *Subscriber {
	return &Subscriber{
		ID:            id,
		Channels:      make(map[string]bool),
		ShardChannels: make(map[string]bool),
		Patterns:      make(map[string]bool),
		MessageCh:     make(chan *Message, 100),
	}
}

// Subscribe 订阅频道
func (psm *PubSubManager) Subscribe(subscriber *Subscriber, channels ...string) []string {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	psm.subscribers[subscriber] = true
	subscribed := make([]string, 0)

	for _, channel := range channels {
		subscriber.mu.Lock()
		subscriber.Channels[channel] = true
		subscriber.mu.Unlock()

		if psm.channels[channel] == nil {
			psm.channels[channel] = make(map[*Subscriber]bool)
		}
		psm.channels[channel][subscriber] = true
		subscribed = append(subscribed, channel)
	}

	return subscribed
}

// PSubscribe 订阅模式
func (psm *PubSubManager) PSubscribe(subscriber *Subscriber, patterns ...string) []string {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	psm.subscribers[subscriber] = true
	subscribed := make([]string, 0)

	for _, pattern := range patterns {
		subscriber.mu.Lock()
		subscriber.Patterns[pattern] = true
		subscriber.mu.Unlock()

		if psm.patterns[pattern] == nil {
			psm.patterns[pattern] = make(map[*Subscriber]bool)
		}
		psm.patterns[pattern][subscriber] = true
		subscribed = append(subscribed, pattern)
	}

	return subscribed
}

// SSubscribe 订阅 shard 频道（Redis 7+ SSUBSCRIBE）。
// shard 频道与普通频道命名空间独立：SPUBLISH 只投递给 shard 订阅者。
func (psm *PubSubManager) SSubscribe(subscriber *Subscriber, channels ...string) []string {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	psm.subscribers[subscriber] = true
	subscribed := make([]string, 0)

	for _, channel := range channels {
		subscriber.mu.Lock()
		subscriber.ShardChannels[channel] = true
		subscriber.mu.Unlock()

		if psm.shardChannels[channel] == nil {
			psm.shardChannels[channel] = make(map[*Subscriber]bool)
		}
		psm.shardChannels[channel][subscriber] = true
		subscribed = append(subscribed, channel)
	}

	return subscribed
}

// SUnsubscribe 取消订阅 shard 频道。
func (psm *PubSubManager) SUnsubscribe(subscriber *Subscriber, channels ...string) []string {
	return psm.sUnsubscribeInternal(subscriber, channels...)
}

// sUnsubscribeInternal 取消 shard 频道订阅（不持有锁调用）。
func (psm *PubSubManager) sUnsubscribeInternal(subscriber *Subscriber, channels ...string) []string {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	return psm.sUnsubscribeLocked(subscriber, channels...)
}

// sUnsubscribeLocked 在持有 psm.mu 时取消 shard 频道订阅。
func (psm *PubSubManager) sUnsubscribeLocked(subscriber *Subscriber, channels ...string) []string {
	unsubscribed := make([]string, 0)
	if len(channels) == 0 {
		subscriber.mu.Lock()
		for ch := range subscriber.ShardChannels {
			channels = append(channels, ch)
		}
		subscriber.mu.Unlock()
	}

	for _, channel := range channels {
		subscriber.mu.Lock()
		if !subscriber.ShardChannels[channel] {
			subscriber.mu.Unlock()
			continue
		}
		delete(subscriber.ShardChannels, channel)
		subscriber.mu.Unlock()

		if subs, exists := psm.shardChannels[channel]; exists {
			delete(subs, subscriber)
			if len(subs) == 0 {
				delete(psm.shardChannels, channel)
			}
		}
		unsubscribed = append(unsubscribed, channel)
	}

	return unsubscribed
}

// SPublish 发布消息到 shard 频道（Redis 7+ SPUBLISH），返回接收者数量。
func (psm *PubSubManager) SPublish(channel string, message []byte) int {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	count := 0
	msg := &Message{
		Channel: channel,
		Data:    message,
		Shard:   true,
	}

	if subs, exists := psm.shardChannels[channel]; exists {
		for sub := range subs {
			sub.closeMu.Lock()
			if sub.closed {
				sub.closeMu.Unlock()
				continue
			}
			select {
			case sub.MessageCh <- msg:
				count++
			default:
				logger.Logger.Warn().
					Str("subscriber_id", sub.ID).
					Str("shard_channel", channel).
					Msg("订阅者消息通道已满，跳过 shard 消息")
			}
			sub.closeMu.Unlock()
		}
	}

	return count
}

// Unsubscribe 取消订阅频道
func (psm *PubSubManager) Unsubscribe(subscriber *Subscriber, channels ...string) []string {
	return psm.unsubscribeInternal(subscriber, channels...)
}

// unsubscribeInternal 内部实现，不持有psm.mu锁
func (psm *PubSubManager) unsubscribeInternal(subscriber *Subscriber, channels ...string) []string {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	return psm.unsubscribeLocked(subscriber, channels...)
}

// unsubscribeLocked 实际执行取消订阅，要求已持有psm.mu锁
func (psm *PubSubManager) unsubscribeLocked(subscriber *Subscriber, channels ...string) []string {
	unsubscribed := make([]string, 0)

	if len(channels) == 0 {
		// 取消所有订阅
		subscriber.mu.RLock()
		for channel := range subscriber.Channels {
			channels = append(channels, channel)
		}
		subscriber.mu.RUnlock()
	}

	for _, channel := range channels {
		subscriber.mu.Lock()
		if subscriber.Channels[channel] {
			delete(subscriber.Channels, channel)
			unsubscribed = append(unsubscribed, channel)
		}
		subscriber.mu.Unlock()

		if subs, exists := psm.channels[channel]; exists {
			delete(subs, subscriber)
			if len(subs) == 0 {
				delete(psm.channels, channel)
			}
		}
	}

	return unsubscribed
}

// PUnsubscribe 取消订阅模式
func (psm *PubSubManager) PUnsubscribe(subscriber *Subscriber, patterns ...string) []string {
	return psm.punsubscribeInternal(subscriber, patterns...)
}

// punsubscribeInternal 内部实现，不持有psm.mu锁
func (psm *PubSubManager) punsubscribeInternal(subscriber *Subscriber, patterns ...string) []string {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	return psm.punsubscribeLocked(subscriber, patterns...)
}

// punsubscribeLocked 实际执行取消模式订阅，要求已持有psm.mu锁
func (psm *PubSubManager) punsubscribeLocked(subscriber *Subscriber, patterns ...string) []string {
	unsubscribed := make([]string, 0)

	if len(patterns) == 0 {
		// 取消所有模式订阅
		subscriber.mu.RLock()
		for pattern := range subscriber.Patterns {
			patterns = append(patterns, pattern)
		}
		subscriber.mu.RUnlock()
	}

	for _, pattern := range patterns {
		subscriber.mu.Lock()
		if subscriber.Patterns[pattern] {
			delete(subscriber.Patterns, pattern)
			unsubscribed = append(unsubscribed, pattern)
		}
		subscriber.mu.Unlock()

		if subs, exists := psm.patterns[pattern]; exists {
			delete(subs, subscriber)
			if len(subs) == 0 {
				delete(psm.patterns, pattern)
			}
		}
	}

	return unsubscribed
}

// Publish 发布消息
func (psm *PubSubManager) Publish(channel string, message []byte) int {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	count := 0
	msg := &Message{
		Channel: channel,
		Data:    message,
	}

	// 发送给频道订阅者
	if subs, exists := psm.channels[channel]; exists {
		for sub := range subs {
			sub.closeMu.Lock()
			if sub.closed {
				sub.closeMu.Unlock()
				continue
			}
			select {
			case sub.MessageCh <- msg:
				count++
			default:
				logger.Logger.Warn().
					Str("subscriber_id", sub.ID).
					Str("channel", channel).
					Msg("订阅者消息通道已满，跳过消息")
			}
			sub.closeMu.Unlock()
		}
	}

	// 发送给模式订阅者
	for pattern, subs := range psm.patterns {
		if matchPattern(channel, pattern) {
			patternMsg := &Message{
				Channel: channel,
				Pattern: pattern,
				Data:    message,
			}
			for sub := range subs {
				sub.closeMu.Lock()
				if sub.closed {
					sub.closeMu.Unlock()
					continue
				}
				select {
				case sub.MessageCh <- patternMsg:
					count++
				default:
					logger.Logger.Warn().
						Str("subscriber_id", sub.ID).
						Str("pattern", pattern).
						Msg("订阅者消息通道已满，跳过消息")
				}
				sub.closeMu.Unlock()
			}
		}
	}

	return count
}

// GetSubscriberCount 获取订阅者数量
func (psm *PubSubManager) GetSubscriberCount(channel string) int {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	if subs, exists := psm.channels[channel]; exists {
		return len(subs)
	}
	return 0
}

// RemoveSubscriber 移除订阅者
func (psm *PubSubManager) RemoveSubscriber(subscriber *Subscriber) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	subscriber.closeMu.Lock()
	if subscriber.closed {
		subscriber.closeMu.Unlock()
		return
	}
	subscriber.closed = true
	close(subscriber.MessageCh)
	subscriber.closeMu.Unlock()

	// 取消所有频道订阅
	subscriber.mu.RLock()
	channels := make([]string, 0, len(subscriber.Channels))
	for channel := range subscriber.Channels {
		channels = append(channels, channel)
	}
	patterns := make([]string, 0, len(subscriber.Patterns))
	for pattern := range subscriber.Patterns {
		patterns = append(patterns, pattern)
	}
	shardChannels := make([]string, 0, len(subscriber.ShardChannels))
	for channel := range subscriber.ShardChannels {
		shardChannels = append(shardChannels, channel)
	}
	subscriber.mu.RUnlock()

	psm.unsubscribeLocked(subscriber, channels...)
	psm.punsubscribeLocked(subscriber, patterns...)
	psm.sUnsubscribeLocked(subscriber, shardChannels...)

	delete(psm.subscribers, subscriber)
}

// Clear 清空所有订阅状态，用于测试隔离
func (psm *PubSubManager) Clear() {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	// 关闭所有订阅者的消息通道并移除订阅者
	for sub := range psm.subscribers {
		sub.closeMu.Lock()
		if sub.closed {
			sub.closeMu.Unlock()
			continue
		}
		sub.closed = true
		close(sub.MessageCh)
		sub.closeMu.Unlock()
	}

	// 重置所有映射
	psm.channels = make(map[string]map[*Subscriber]bool)
	psm.shardChannels = make(map[string]map[*Subscriber]bool)
	psm.patterns = make(map[string]map[*Subscriber]bool)
	psm.subscribers = make(map[*Subscriber]bool)
}

// GetChannels 获取所有频道
func (psm *PubSubManager) GetChannels(pattern string) []string {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	channels := make([]string, 0)
	for channel := range psm.channels {
		if pattern == "" || pattern == "*" || matchPattern(channel, pattern) {
			channels = append(channels, channel)
		}
	}
	return channels
}

// GetShardChannels 获取匹配 pattern 的活跃 shard 频道（PUBSUB SHARDCHANNELS）。
func (psm *PubSubManager) GetShardChannels(pattern string) []string {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	channels := make([]string, 0)
	for channel := range psm.shardChannels {
		if pattern == "" || pattern == "*" || matchPattern(channel, pattern) {
			channels = append(channels, channel)
		}
	}
	return channels
}

// GetPatternCount 获取模式订阅数量
func (psm *PubSubManager) GetPatternCount() int {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	count := 0
	for sub := range psm.subscribers {
		sub.mu.RLock()
		count += len(sub.Patterns)
		sub.mu.RUnlock()
	}
	return count
}
