package txn

import (
	"sync/atomic"
	"time"
)

const deadlineMask = ^int64(0x7)
const errCodeMask = int64(0x3)
const signalInterruptedMask = int64(0x4)

type Status struct {
	status     int64
	scanStatus *scanStatus
}

type scanStatus struct {
	startTime   int64
	interrupted *uint32
	onRetry     func(elapsed time.Duration)
}

func (ts *Status) IsAborted() bool {
	if ts == nil {
		return false
	}
	ts.updateIfNeeded()
	return ts.err() != nil
}

func (ts *Status) Error() error {
	if ts == nil {
		return nil
	}
	ts.updateIfNeeded()
	return ts.err()
}

func (ts *Status) GetDeadlineTime() time.Time {
	if ts == nil {
		return time.Time{}
	}
	deadline := ts.status & deadlineMask
	if deadline == 0 {
		return time.Time{}
	}
	return time.Unix(0, deadline)
}

func (ts *Status) WithDeadline(t time.Time) *Status {
	if t.IsZero() {
		return ts
	}
	if ts == nil {
		return &Status{
			status: t.UnixNano() & deadlineMask,
		}
	}
	existingDeadline := ts.status & deadlineMask
	if existingDeadline != 0 {
		if t.UnixNano() > existingDeadline {
			return ts
		}
	}

	return &Status{
		scanStatus: ts.scanStatus,
		status:     (t.UnixNano() & deadlineMask) | (ts.status & ^deadlineMask),
	}
}

func (ts *Status) Retry() {
	ts.scanStatus.onRetry(time.Since(time.Unix(0, ts.scanStatus.startTime)))
	ts.status = ts.status & ^errCodeMask
	ts.scanStatus.startTime = time.Now().UnixNano()
}

func (ts *Status) updateIfNeeded() {
	errCode := ts.status & errCodeMask
	if errCode != 0 {
		// Already set an error, no update needed
		return
	}

	now := time.Now().UnixNano()
	deadline := ts.status & deadlineMask
	if deadline != 0 {
		// Check if deadline is hit
		if now >= deadline {
			ts.status |= int64(errCodeDeadline)
			return
		}
	}

	if ts.scanStatus != nil {
		// Check if we are interrupted
		interrupted := atomic.LoadUint32(ts.scanStatus.interrupted)
		if interrupted == 1 {
			ts.status = deadline | int64(errCodeInterrupted) | signalInterruptedMask
			return
		}
	}
}

func (ts *Status) err() txnErr {
	errCode := byte(ts.status & errCodeMask)
	switch errCode {
	case errCodeClosed:
		return ClosedError{}
	case errCodeInterrupted:
		return InterruptedError{}
	case errCodeDeadline:
		return DeadlineError{}
	}
	return nil
}
