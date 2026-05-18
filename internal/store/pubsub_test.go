package store

import (
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

// mustReceiveMessage 从订阅者通道接收消息，超时则 fatal
// 使用 time.NewTimer 而非 time.After 以避免 timer 泄漏
func mustReceiveMessage(t *testing.T, ch chan *Message) *Message {
	t.Helper()
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case msg := <-ch:
		return msg
	case <-timer.C:
		t.Fatal("timeout waiting for PubSub message")
		return nil
	}
}

func TestPubSubManagerCreation(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	assert.NotNil(t, psm)
	assert.NotNil(t, psm.channels)
	assert.NotNil(t, psm.patterns)
	assert.NotNil(t, psm.subscribers)
}

func TestSubscriberCreation(t *testing.T) {
	t.Parallel()
	sub := NewSubscriber("test-sub")
	assert.NotNil(t, sub)
	assert.Equal(t, "test-sub", sub.ID)
	assert.NotNil(t, sub.Channels)
	assert.NotNil(t, sub.Patterns)
	assert.NotNil(t, sub.MessageCh)
	assert.Equal(t, 100, cap(sub.MessageCh))
}

func TestSubscribe(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe to channels
	subscribed := psm.Subscribe(sub, "channel1", "channel2")
	assert.Equal(t, 2, len(subscribed))
	assert.Equal(t, true, existsIn(subscribed, "channel1"))
	assert.Equal(t, true, existsIn(subscribed, "channel2"))

	// Check subscriber count
	count := psm.GetSubscriberCount("channel1")
	assert.Equal(t, 1, count)

	// Check subscriber's channel list
	assert.True(t, sub.Channels["channel1"])
	assert.True(t, sub.Channels["channel2"])

	// Subscribe to existing channel again - returns channel even if already subscribed
	subscribed = psm.Subscribe(sub, "channel1")
	assert.Equal(t, 1, len(subscribed))
	assert.Equal(t, true, existsIn(subscribed, "channel1"))
}

// existsIn helper function
func existsIn(slice []string, val string) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func TestPSubscribe(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe to patterns
	subscribed := psm.PSubscribe(sub, "news.*", "events.*")
	assert.Equal(t, 2, len(subscribed))
	assert.Equal(t, true, existsIn(subscribed, "news.*"))
	assert.Equal(t, true, existsIn(subscribed, "events.*"))

	// Check subscriber's pattern list
	assert.True(t, sub.Patterns["news.*"])
	assert.True(t, sub.Patterns["events.*"])
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe first
	psm.Subscribe(sub, "channel1", "channel2")
	assert.True(t, sub.Channels["channel1"])
	assert.True(t, sub.Channels["channel2"])

	// Unsubscribe from one channel
	unsubscribed := psm.Unsubscribe(sub, "channel1")
	assert.Equal(t, 1, len(unsubscribed))
	assert.Equal(t, true, existsIn(unsubscribed, "channel1"))

	// Check state after unsubscribe
	assert.False(t, sub.Channels["channel1"])
	assert.True(t, sub.Channels["channel2"]) // Still subscribed to channel2

	// Unsubscribe from all
	unsubscribed = psm.Unsubscribe(sub) // Empty = all
	assert.Equal(t, 1, len(unsubscribed))
	assert.False(t, sub.Channels["channel2"])
}

func TestPUnsubscribe(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe to patterns first
	psm.PSubscribe(sub, "pattern1", "pattern2")
	assert.True(t, sub.Patterns["pattern1"])
	assert.True(t, sub.Patterns["pattern2"])

	// Unsubscribe from one pattern
	unsubscribed := psm.PUnsubscribe(sub, "pattern1")
	assert.Equal(t, 1, len(unsubscribed))

	// Unsubscribe from all
	unsubscribed = psm.PUnsubscribe(sub)
	assert.Equal(t, 1, len(unsubscribed))
	assert.False(t, sub.Patterns["pattern2"])
}

func TestPublish(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe to channel
	psm.Subscribe(sub, "channel1")

	// Publish message
	count := psm.Publish("channel1", []byte("hello"))
	assert.Equal(t, 1, count)

	// Check message received
	msg := mustReceiveMessage(t, sub.MessageCh)
	assert.Equal(t, "channel1", msg.Channel)
	assert.Equal(t, "hello", string(msg.Data))
	assert.Equal(t, "", msg.Pattern)
}

func TestPublishMultipleSubscribers(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()

	sub1 := NewSubscriber("sub1")
	sub2 := NewSubscriber("sub2")

	psm.Subscribe(sub1, "channel1")
	psm.Subscribe(sub2, "channel1")

	// Publish
	count := psm.Publish("channel1", []byte("hello"))
	assert.Equal(t, 2, count)

	// Both should receive
	for _, sub := range []*Subscriber{sub1, sub2} {
		msg := mustReceiveMessage(t, sub.MessageCh)
		assert.Equal(t, "channel1", msg.Channel)
		assert.Equal(t, "hello", string(msg.Data))
	}
}

func TestPublishPatternMatch(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe to pattern
	psm.PSubscribe(sub, "news.*")

	// Publish to matching channel
	count := psm.Publish("news.sports", []byte("sports news"))
	assert.Equal(t, 1, count)

	// Check message received with pattern
	msg := mustReceiveMessage(t, sub.MessageCh)
	assert.Equal(t, "news.sports", msg.Channel)
	assert.Equal(t, "news.*", msg.Pattern)
	assert.Equal(t, "sports news", string(msg.Data))
}

func TestPublishNoSubscribers(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()

	// Publish to channel with no subscribers
	count := psm.Publish("empty-channel", []byte("hello"))
	assert.Equal(t, 0, count)
}

func TestPublishChannelAndPattern(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe to both channel and pattern
	psm.Subscribe(sub, "testchannel")
	psm.PSubscribe(sub, "test*")

	// Publish to matching channel - should send to both subscriptions
	count := psm.Publish("testchannel", []byte("hello"))
	assert.Equal(t, 2, count) // One for direct, one for pattern

	// Should receive two messages
	for range 2 {
		msg := mustReceiveMessage(t, sub.MessageCh)
		assert.Equal(t, "testchannel", msg.Channel)
		assert.Equal(t, "hello", string(msg.Data))
	}
}

func TestRemoveSubscriber(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Subscribe
	psm.Subscribe(sub, "channel1")
	psm.PSubscribe(sub, "pattern*")

	// Verify subscriptions exist
	assert.Equal(t, 1, psm.GetSubscriberCount("channel1"))

	// Remove subscriber
	psm.RemoveSubscriber(sub)

	// Verify removed
	assert.Equal(t, 0, psm.GetSubscriberCount("channel1"))
	assert.False(t, sub.Channels["channel1"])
	assert.False(t, sub.Patterns["pattern*"])

	// Channel should be closed
	_, ok := <-sub.MessageCh
	assert.False(t, ok) // Channel should be closed
}

func TestGetChannels(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()

	// Initially empty
	channels := psm.GetChannels("")
	assert.Equal(t, 0, len(channels))

	// Add some channels
	psm.Subscribe(NewSubscriber("sub1"), "channel1", "channel2", "other")

	// Get all channels
	channels = psm.GetChannels("")
	assert.Equal(t, 3, len(channels))

	// Get matching pattern
	channels = psm.GetChannels("channel*")
	assert.Equal(t, 2, len(channels))

	// Get specific channel
	channels = psm.GetChannels("channel1")
	assert.Equal(t, 1, len(channels))
	assert.Equal(t, "channel1", channels[0])
}

func TestConcurrentSubscribe(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	var wg sync.WaitGroup

	// Concurrent subscriptions
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(ch string) {
			defer wg.Done()
			psm.Subscribe(sub, ch)
		}("channel" + string(rune('a'+i)))
	}
	wg.Wait()

	// Check all subscriptions exist
	psm.mu.RLock()
	count := len(sub.Channels)
	psm.mu.RUnlock()
	assert.Equal(t, 10, count)
}

func TestConcurrentPublish(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")
	psm.Subscribe(sub, "channel")

	var wg sync.WaitGroup
	var mu sync.Mutex
	published := 0

	// Concurrent publishes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			count := psm.Publish("channel", []byte("msg"))
			mu.Lock()
			published += count
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// Should receive all messages (or some may be dropped if channel is full)
	mu.Lock()
	assert.Equal(t, 10, published)
	mu.Unlock()
}

func TestSubscriberCount(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()

	// Initially 0
	count := psm.GetSubscriberCount("channel1")
	assert.Equal(t, 0, count)

	// Add subscribers
	sub1 := NewSubscriber("sub1")
	sub2 := NewSubscriber("sub2")

	psm.Subscribe(sub1, "channel1")
	assert.Equal(t, 1, psm.GetSubscriberCount("channel1"))

	psm.Subscribe(sub2, "channel1")
	assert.Equal(t, 2, psm.GetSubscriberCount("channel1"))

	// Non-existent channel
	count = psm.GetSubscriberCount("nonexistent")
	assert.Equal(t, 0, count)
}

func TestMessageChannelBuffering(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")
	psm.Subscribe(sub, "channel")

	// Fill up the message channel (buffer is 100)
	for i := 0; i < 100; i++ {
		psm.Publish("channel", []byte("msg"))
	}

	// One more should be dropped (channel full)
	count := psm.Publish("channel", []byte("full"))
	assert.Equal(t, 0, count) // Should be dropped
}

func TestUnsubscribeFromNonSubscribedChannel(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Unsubscribe from channel never subscribed to
	unsubscribed := psm.Unsubscribe(sub, "neversubscribed")
	assert.Equal(t, 0, len(unsubscribed))
}

func TestPUnsubscribeFromNonSubscribedPattern(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub := NewSubscriber("sub1")

	// Unsubscribe from pattern never subscribed to
	unsubscribed := psm.PUnsubscribe(sub, "neverpattern")
	assert.Equal(t, 0, len(unsubscribed))
}

// TestGetPatternCount tests GetPatternCount function
func TestGetPatternCount(t *testing.T) {
	t.Parallel()
	psm := NewPubSubManager()
	sub1 := NewSubscriber("sub1")
	sub2 := NewSubscriber("sub2")

	// Initially, no pattern subscriptions
	count := psm.GetPatternCount()
	assert.Equal(t, 0, count)

	// Subscribe to patterns
	psm.PSubscribe(sub1, "news.*")
	psm.PSubscribe(sub2, "news.*")
	psm.PSubscribe(sub1, "sports.*")

	// Now should have 3 pattern subscriptions (sub1 has 2, sub2 has 1)
	count = psm.GetPatternCount()
	assert.Equal(t, 3, count)
}
