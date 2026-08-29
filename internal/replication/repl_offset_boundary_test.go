package replication

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

// The offset a master advertises in +FULLRESYNC must be the backlog's own
// write watermark, so that the bytes following it are whole commands.
//
// Three consumers take the value literally:
//   - +FULLRESYNC <replid> <offset>, which the replica stores verbatim as its
//     own lastOffset (reconnect.go sendPSYNC)
//   - backlog.GetRange(snapshotOffset, currentOffset): the replica's byte stream
//     starts exactly at that offset
//   - PSYNC CONTINUE availability/boundary checks, which used to compare the
//     request against a separate counter while ranging the bytes off the ring
//
// PropagateCommand used to maintain `masterReplOffset += len(cmdBytes)` as a
// second, unlocked step after backlog.Append, so the advertised value and the
// ring were two timelines. This guard is expressed as a property of the
// advertised value rather than as GetMasterReplOffset() ==
// backlog.GetCurrentOffset(): with concurrent writers those two reads are not
// an atomic pair and legitimately differ by whatever appended in between, so an
// equality assertion would test the sampler instead of the implementation.
//
// Measured on the pre-fix code over 6000 FULLRESYNC windows: 4 advertised an
// offset the ring could not serve as whole commands. Post-fix: 0 over the same
// sample size (expect ~12 at the pre-fix rate if the defect were still there).
// See docs/failures/repl-offset-boundary-drift.md.
func TestFullresyncAdvertisedOffsetIsServable(t *testing.T) {
	t.Parallel()

	s, err := store.NewBadgerStore(t.TempDir())
	assert.NoError(t, err)
	defer s.Close()

	rm := NewReplicationManager(s)
	defer rm.Stop()
	rm.SetRole(RoleMaster)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				// Varying command lengths, so a stale offset landing inside a
				// command is not accidentally aligned with a boundary.
				rm.PropagateCommand(replCmd("LPUSH", "gap:k", fmt.Sprintf("%d-%d", w, i)))
			}
		}(w)
	}

	backlog := rm.GetBacklog()
	windows := 0
	unservable := 0
	firstBad := int64(0)
	var firstReason string

	for i := 0; i < 2000; i++ {
		// The handler's FULLRESYNC sequence, snapshot lock included.
		s.SnapshotMuLock()
		snapshotOffset := rm.GetMasterReplOffset()
		_, err := GenerateRDBWithSnapshotLock(s)
		currentOffset := rm.GetMasterReplOffset()
		s.SnapshotMuUnlock()
		assert.NoError(t, err)

		if currentOffset <= snapshotOffset {
			continue
		}
		windows++

		gap, err := backlog.GetRange(snapshotOffset, currentOffset)
		if err != nil {
			unservable++
			if firstBad == 0 {
				firstBad, firstReason = snapshotOffset, err.Error()
			}
			continue
		}
		if len(gap) == 0 || gap[0] != '*' {
			unservable++
			if firstBad == 0 {
				firstBad, firstReason = snapshotOffset, fmt.Sprintf("stream starts with %q, not '*'", gap[0])
			}
		}
	}
	close(stop)
	wg.Wait()

	t.Logf("FULLRESYNC windows with a non-empty gap: %d", windows)
	if unservable > 0 {
		t.Errorf("%d/%d FULLRESYNC windows advertised an offset that could not be served as whole "+
			"commands (first snapshotOffset=%d: %s) — the replica stores that offset as its lastOffset "+
			"and ReadRESP mis-frames its entire stream", unservable, windows, firstBad, firstReason)
	}
}
