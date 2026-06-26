package metrics

import (
	"sync"
	"time"

	"github.com/lbp0200/BoltDB/internal/replication"
)

type Collector struct {
	mu sync.RWMutex

	RetryMetricsFn     func() (activeRetries, totalRetries, writesBlocked, l0Rejected, l0Delayed int64, l0Score float64)
	L0ScoreFn          func() float64
	MasterReplOffsetFn func() int64
	SlaveReplOffsetFn  func() int64
	ReconnectCountFn   func() int64
	SlaveCountFn       func() int
	BacklogSizeFn      func() int64
	BacklogAvailFn     func() int64
	RoleFn             func() string
	ActiveClientsFn    func() int
	BlockedClientsFn   func() int
	MonitorClientsFn   func() int
	PubSubClientsFn    func() int
	PubSubSubsFn       func() int
	TotalOutputBytesFn func() int64

	lastSnapshot Snapshot
	snapshotAt   time.Time
}

func NewCollector() *Collector {
	return &Collector{}
}

func (c *Collector) Snapshot() Snapshot {
	c.mu.RLock()
	s := c.lastSnapshot
	at := c.snapshotAt
	c.mu.RUnlock()

	if time.Since(at) < time.Second {
		return s
	}

	return c.refresh()
}

func (c *Collector) refresh() Snapshot {
	goroutines, mem := CollectRuntime()

	var retryActive, retryTotal, writesBlocked, l0Rejected, l0Delayed int64
	var l0Score float64

	// Read function pointers under RLock to prevent data race
	// if any pointer is being set concurrently (e.g. during init).
	c.mu.RLock()
	if c.RetryMetricsFn != nil {
		retryActive, retryTotal, writesBlocked, l0Rejected, l0Delayed, l0Score = c.RetryMetricsFn()
	}
	if c.L0ScoreFn != nil && l0Score == 0 {
		l0Score = c.L0ScoreFn()
	}

	masterOff := int64(0)
	slaveOff := int64(0)
	lag := int64(0)
	reconns := int64(0)
	slaveN := 0
	blSize := int64(0)
	blAvail := int64(0)
	role := replication.RoleMaster
	if c.MasterReplOffsetFn != nil {
		masterOff = c.MasterReplOffsetFn()
	}
	if c.SlaveReplOffsetFn != nil {
		slaveOff = c.SlaveReplOffsetFn()
	}
	if c.ReconnectCountFn != nil {
		reconns = c.ReconnectCountFn()
	}
	if c.SlaveCountFn != nil {
		slaveN = c.SlaveCountFn()
	}
	if c.BacklogSizeFn != nil {
		blSize = c.BacklogSizeFn()
	}
	if c.BacklogAvailFn != nil {
		blAvail = c.BacklogAvailFn()
	}
	if c.RoleFn != nil {
		role = c.RoleFn()
	}

	if role == replication.RoleSlave {
		lag = masterOff - slaveOff
	}

	activeC := 0
	blockedC := 0
	monC := 0
	pubC := 0
	pubS := 0
	outB := int64(0)
	if c.ActiveClientsFn != nil {
		activeC = c.ActiveClientsFn()
	}
	if c.BlockedClientsFn != nil {
		blockedC = c.BlockedClientsFn()
	}
	if c.MonitorClientsFn != nil {
		monC = c.MonitorClientsFn()
	}
	if c.PubSubClientsFn != nil {
		pubC = c.PubSubClientsFn()
	}
	if c.PubSubSubsFn != nil {
		pubS = c.PubSubSubsFn()
	}
	if c.TotalOutputBytesFn != nil {
		outB = c.TotalOutputBytesFn()
	}
	c.mu.RUnlock()

	s := Snapshot{
		Time: time.Now(),

		ActiveRetries: retryActive,
		TotalRetries:  retryTotal,
		WritesBlocked: writesBlocked,
		L0Rejected:    l0Rejected,
		L0Delayed:     l0Delayed,
		L0Score:       l0Score,

		Goroutines:  goroutines,
		AllocBytes:  mem.Alloc,
		HeapObjects: mem.HeapObjects,
		HeapInuse:   mem.HeapInuse,
		StackInuse:  mem.StackInuse,
		NumGC:       mem.NumGC,
		LastGC:      formatLastGC(mem.LastGC),

		MasterReplOffset: masterOff,
		SlaveReplOffset:  slaveOff,
		ReplicationLag:   lag,
		ReconnectCount:   reconns,
		SlaveCount:       slaveN,
		BacklogSize:      blSize,
		BacklogAvailable: blAvail,
		Role:             role,

		ActiveClients:    activeC,
		BlockedClients:   blockedC,
		MonitorClients:   monC,
		PubSubClients:    pubC,
		PubSubSubs:       pubS,
		TotalOutputBytes: outB,
	}

	c.mu.Lock()
	c.lastSnapshot = s
	c.snapshotAt = time.Now()
	c.mu.Unlock()

	return s
}
