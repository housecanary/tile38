package txn

import (
	"sync/atomic"
	"time"
)

type voidType struct{}

var void = voidType{}

type Scheduler struct {
	interrupt     uint32
	readBarrier   uint32
	inflightReads uint64

	readRequests  chan voidType
	writeRequests chan voidType
	readPermit    chan voidType
	writePermit   chan voidType
	opComplete    chan time.Duration
}

func NewScheduler(initialWriteDelay time.Duration, maxReadDelay time.Duration) (*Scheduler, func()) {
	s := &Scheduler{
		readRequests:  make(chan voidType),
		writeRequests: make(chan voidType),
		readPermit:    make(chan voidType),
		writePermit:   make(chan voidType),
		opComplete:    make(chan time.Duration),
	}
	done := make(chan voidType)
	go s.schedule(done, initialWriteDelay, maxReadDelay)
	return s, func() {
		close(done)
	}
}

func (s *Scheduler) Write() (done func()) {
	s.writeRequests <- void
	<-s.writePermit
	return s.opDone
}

func (s *Scheduler) Read() (done func()) {
	s.readRequests <- void
	<-s.readPermit
	return s.opDone
}

func (s *Scheduler) Scan() (done func(), status *Status) {
	return s.Read(), &Status{
		scanStatus: &scanStatus{
			startTime:   time.Now().UnixNano(),
			interrupted: &s.interrupt,
			onRetry:     s.opInterrupted,
		},
	}
}

func (s *Scheduler) schedule(done chan voidType, writeDelay time.Duration, maxReadDelay time.Duration) {
	inflight := 0
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

scheduler:
	for {
		// read phase
		//
		// execute until write request received
		//
		// on read request: allow request, increment inflight
		// on complete: decrement inflight
		// on write request: set timer for for interrupting reads
	read:
		for {
			select {
			case <-s.readRequests:
				inflight++
				s.readPermit <- void
			case <-s.opComplete:
				inflight--
			case <-s.writeRequests:
				timer.Reset(writeDelay)
				break read
			case <-done:
				break scheduler
			}
		}

		// prepare write phase
		//
		// execute until number of inflight requests is 0 OR interrupt timer expires
		//
		// on read request: allow request, increment inflight
		// on complete: decrement inflight
	prepareWrite:
		for {
			if inflight == 0 {
				if !timer.Stop() {
					<-timer.C
				}
				break prepareWrite
			}
			select {
			case <-s.readRequests:
				inflight++
				s.readPermit <- void
			case <-s.opComplete:
				inflight--
			case <-timer.C:
				break prepareWrite
			case <-done:
				break scheduler
			}
		}

		// wait reads done phase
		//
		// execute until number of inflight requests is 0
		//
		// on complete: decrement inflight
		maxRuntime := time.Duration(-1)
		atomic.StoreUint32(&s.interrupt, 1)
	waitReadsDone:
		for {
			if inflight == 0 {
				break waitReadsDone
			}
			select {
			case runtime := <-s.opComplete:
				inflight--
				if runtime > maxRuntime {
					maxRuntime = runtime
				}
			case <-done:
				break scheduler
			}
		}
		atomic.StoreUint32(&s.interrupt, 0)
		if maxRuntime > writeDelay {
			// If a scan took > 1/2 of the current write delay, before interruption
			// extend write delay by doubling max runtime
			writeDelay = maxRuntime * 2
		} else if maxRuntime == -1 {
			// If no scans interrupted, shrink writeDelay by 25%
			writeDelay = writeDelay / 4 * 3
		}

		if writeDelay > 1*time.Minute {
			writeDelay = 1 * time.Minute
		} else if writeDelay < 1*time.Millisecond {
			writeDelay = 1 * time.Millisecond
		}

		// write phase
		//
		// execute until read request received
		//
		// on read request: set timer for interrupting writes
		// on write request: allow request, wait for complete

		// execute the write request that caused us to enter writePhase
		s.writePermit <- void
		<-s.opComplete
	write:
		for {
			select {
			case <-s.readRequests:
				timer.Reset(maxReadDelay)
				break write
			case <-s.writeRequests:
				s.writePermit <- void
				<-s.opComplete
			case <-done:
				break scheduler
			}
		}

		// prepare read phase
		//
		// execute until there are no enqueued write requests OR interrupt timer expires
		//
		// on write request (nonblocking): allow request, wait for complete
		// on interrupt timer expires: set flag to interrupt executing reads
	prepareRead:
		for {
			// check timer
			select {
			case <-timer.C:
				break prepareRead
			default:
			}

			select {
			case <-s.writeRequests:
				s.writePermit <- void
				<-s.opComplete
			default:
				// no pending writes, exit this phase
				if !timer.Stop() {
					<-timer.C
				}
				break prepareRead
			}
		}

		// start the pending read and repeat the loop
		inflight++
		s.readPermit <- void
	}

	for inflight > 0 {
		<-s.opComplete
		inflight--
	}
}

func (s *Scheduler) opDone() {
	s.opComplete <- -1
}

func (s *Scheduler) opInterrupted(runtime time.Duration) {
	s.opComplete <- runtime
	s.Read()
}
