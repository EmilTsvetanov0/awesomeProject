package orchestrator

import (
	"context"
	"github.com/looplab/fsm"
	"log"
	"orchestrator/internal/kafka"
	"sync"
	"time"
)

/* ---------- 1. Константы состояний и событий ---------- */

const (
	StInitStartup          = "init_startup"
	StInStartupProcessing  = "in_startup_processing"
	StActive               = "active"
	StInitShutdown         = "init_shutdown"
	StInShutdownProcessing = "in_shutdown_processing"
	StInactive             = "inactive"

	// how long мы ждём heartbeat прежде, чем считать runner «упавшим»
	HeartbeatTTL = 5 * time.Second
	MaxHBRetries = 3 // N попыток «достучаться» до runner‑а
)

/* ---------- 2. Тип Scenario ---------- */

type Scenario struct {
	ID       string
	FSM      *fsm.FSM
	hbMu     sync.Mutex // защита lastHB
	lastHB   time.Time  // время последнего heartbeat
	cancelHB context.CancelFunc
}

/* ---------- 3. Конструктор ---------- */

func NewScenario(id string, enterState func(ctx context.Context, id string, dst string)) *Scenario {
	s := &Scenario{ID: id}
	s.Reset()

	s.FSM = fsm.NewFSM(
		StInitStartup,
		fsm.Events{
			{Name: "begin_startup", Src: []string{StInitStartup, StInactive}, Dst: StInStartupProcessing},
			{Name: "complete_startup", Src: []string{StInStartupProcessing}, Dst: StActive},
			{Name: "begin_shutdown", Src: []string{StActive, StInStartupProcessing}, Dst: StInitShutdown},
			{Name: "process_shutdown", Src: []string{StInitShutdown}, Dst: StInShutdownProcessing},
			{Name: "complete_shutdown", Src: []string{StInShutdownProcessing}, Dst: StInactive},
		},
		fsm.Callbacks{
			// ---- запуск ----
			"enter_in_startup_processing": s.cbEnterStartupProcessing,
			"before_complete_startup":     s.cbBeforeCompleteStartup,

			// ---- остановка ----
			"enter_init_shutdown":          s.cbEnterInitShutdown,
			"enter_in_shutdown_processing": s.cbEnterInShutdownProcessing,
			"enter_inactive":               s.cbEnterInactive,

			// ---- для логов и сохранения состояния
			"enter_state": func(ctx context.Context, e *fsm.Event) {
				//kafka.ChangeState(s.ID, e.Dst)
				enterState(ctx, s.ID, e.Dst) //Фиксируем изменение в постгресе
				log.Printf("[orchestrator] [FSM] %s : %s ➜ %s  (by %s)", s.ID, e.Src, e.Dst, e.Event)
			},
		},
	)
	return s
}

/* ---------- 4. Колбеки FSM ---------- */

func (s *Scenario) cbEnterStartupProcessing(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] entering startup processing, current state: %s",
		s.ID, s.FSM.Current())

	s.stopWatchdog()

	kafka.StartRunner(s.ID)

	watchdogCtx, cancel := context.WithCancel(context.Background())
	s.cancelHB = cancel
	go s.heartbeatWatchdog(watchdogCtx)

	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := s.FSM.Event(context.Background(), "complete_startup"); err != nil {
			log.Printf("[orchestrator] [%s] complete_startup error: %v", s.ID, err)
		}
	}()
}

func (s *Scenario) cbBeforeCompleteStartup(ctx context.Context, e *fsm.Event) {
	if !s.waitForHeartbeat(MaxHBRetries) {
		log.Printf("[orchestrator] [%s] heartbeat not received, triggering shutdown", s.ID)

		if s.FSM.Current() == StInStartupProcessing {
			// Только если ещё не ушли из состояния
			e.Cancel(nil)

			go func() {
				time.Sleep(100 * time.Millisecond)
				if err := s.FSM.Event(context.Background(), "begin_shutdown"); err != nil {
					log.Printf("[orchestrator] [%s] begin_shutdown triggered with error: %v", s.ID, err)
				}
			}()
		}
	}
}

func (s *Scenario) cbEnterInitShutdown(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] initiating shutdown, current state: %s",
		s.ID, s.FSM.Current())
	kafka.StopRunner(s.ID)
	// → _sendStopCommand()_
	// останавливаем watchdog
	s.stopWatchdog()
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := s.FSM.Event(context.Background(), "process_shutdown"); err != nil {
			log.Printf("[orchestrator] [%s] process_shutdown error: %v", s.ID, err)
		}
	}()
}

func (s *Scenario) cbEnterInShutdownProcessing(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] shutdown processing done, marking inactive…", s.ID)

	time.Sleep(1 * time.Second)

	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := s.FSM.Event(context.Background(), "complete_shutdown"); err != nil {
			log.Printf("[orchestrator] [%s] complete_shutdown failed: %v", s.ID, err)
		}
	}()
}

func (s *Scenario) cbEnterInactive(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] scenario is now inactive", s.ID)

	s.stopWatchdog()
}

/* ---------- 5. Heartbeat‑API для Runner‑а ---------- */

// Runner присылает heartbeat → orchestrator вызывает этот метод.
func (s *Scenario) AcceptHeartbeat() {
	s.hbMu.Lock()
	s.lastHB = time.Now()
	s.hbMu.Unlock()
}

/* ---------- 6. Внутренняя логика ---------- */

func (s *Scenario) Reset() {
	s.hbMu.Lock()
	s.lastHB = time.Time{}
	s.hbMu.Unlock()
}

func (s *Scenario) waitForHeartbeat(maxRetries int) bool {
	for i := 0; i < maxRetries; i++ {
		if s.IsOk() {
			return true
		}
		log.Printf("[orchestrator] [%s] no heartbeat, retry %d…", s.ID, i+1)
		time.Sleep(HeartbeatTTL)
	}
	return false
}

func (s *Scenario) IsOk() bool {
	s.hbMu.Lock()
	ok := time.Since(s.lastHB) <= HeartbeatTTL
	s.hbMu.Unlock()
	if ok {
		return true
	}
	return false
}

func (s *Scenario) stopWatchdog() {
	if s.cancelHB != nil {
		s.cancelHB()
		s.cancelHB = nil
	}
}

func (s *Scenario) heartbeatWatchdog(ctx context.Context) {
	log.Printf("[orchestrator] [FSM] [%s] heartbeat watchdog running", s.ID)
	t := time.NewTicker(HeartbeatTTL)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.hbMu.Lock()
			lastHB := s.lastHB
			s.hbMu.Unlock()

			expired := time.Since(lastHB) > HeartbeatTTL

			currentState := s.FSM.Current()
			if expired && (currentState == StActive || currentState == StInStartupProcessing) &&
				currentState != StInitShutdown &&
				currentState != StInShutdownProcessing {
				log.Printf("[orchestrator] [%s] watchdog check: state=%s, lastHB=%v",
					s.ID, s.FSM.Current(), time.Since(s.lastHB))

				log.Printf("[orchestrator] [%s] heartbeat lost ➜ restart runner", s.ID)
				_ = s.FSM.Event(context.Background(), "begin_shutdown")
			}
		}
	}
}
