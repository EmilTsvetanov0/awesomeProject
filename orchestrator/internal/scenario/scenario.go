package orchestrator

import (
	"context"
	"fmt"
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

	s.FSM = fsm.NewFSM(
		StInitStartup,
		fsm.Events{
			{Name: "begin_startup", Src: []string{StInitStartup, StInactive}, Dst: StInStartupProcessing},
			{Name: "complete_startup", Src: []string{StInStartupProcessing}, Dst: StActive},
			{Name: "begin_shutdown", Src: []string{StActive}, Dst: StInitShutdown},
			{Name: "process_shutdown", Src: []string{StInitShutdown}, Dst: StInShutdownProcessing},
			{Name: "complete_shutdown", Src: []string{StInShutdownProcessing}, Dst: StInactive},
		},
		fsm.Callbacks{
			// ---- запуск ----
			"enter_in_startup_processing": s.cbEnterStartupProcessing,
			"before_complete_startup":     s.cbBeforeCompleteStartup,

			// ---- остановка ----
			"enter_init_shutdown":      s.cbEnterInitShutdown,
			"before_complete_shutdown": s.cbBeforeCompleteShutdown,

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
	log.Printf("[orchestrator] [%s] sending START to runner…", s.ID)

	kafka.StartRunner(s.ID)
	// → _sendStartCommand()_  (сокращено)
	// запускаем локальный монитор heartbeat
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelHB = cancel
	go s.heartbeatWatchdog(ctx)

	// сразу (асинхронно) просим FSM попробовать перейти в Active
	go func() {
		if err := s.FSM.Event(ctx, "complete_startup"); err != nil {
			log.Printf("[orchestrator] [%s] complete_startup canceled: %v", s.ID, err)
		}
	}()
}

func (s *Scenario) cbBeforeCompleteStartup(ctx context.Context, e *fsm.Event) {
	if !s.waitForHeartbeat(MaxHBRetries) {
		e.Cancel(fmt.Errorf("runner didn’t respond"))
	}
}

func (s *Scenario) cbEnterInitShutdown(ctx context.Context, e *fsm.Event) {
	log.Printf("[orchestrator] [%s] sending STOP to runner…", s.ID)
	kafka.StopRunner(s.ID)
	// → _sendStopCommand()_
	// останавливаем watchdog
	if s.cancelHB != nil {
		s.cancelHB()
	}
	go func() {
		_ = s.FSM.Event(ctx, "process_shutdown")
	}()
}

func (s *Scenario) cbBeforeCompleteShutdown(ctx context.Context, e *fsm.Event) {
	// ждём подтверждение корректной остановки (упрощённо)
	time.Sleep(1 * time.Second)
}

/* ---------- 5. Heartbeat‑API для Runner‑а ---------- */

// Runner присылает heartbeat → orchestrator вызывает этот метод.
func (s *Scenario) AcceptHeartbeat() {
	s.hbMu.Lock()
	s.lastHB = time.Now()
	s.hbMu.Unlock()
}

/* ---------- 6. Внутренняя логика ---------- */

func (s *Scenario) waitForHeartbeat(maxRetries int) bool {
	for i := 0; i < maxRetries; i++ {
		s.hbMu.Lock()
		ok := time.Since(s.lastHB) <= HeartbeatTTL
		s.hbMu.Unlock()
		if ok {
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

func (s *Scenario) heartbeatWatchdog(ctx context.Context) {
	t := time.NewTicker(HeartbeatTTL)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.hbMu.Lock()
			expired := time.Since(s.lastHB) > HeartbeatTTL
			s.hbMu.Unlock()
			if expired && s.FSM.Is(StActive) {
				log.Printf("[orchestrator] [%s] heartbeat lost ➜ restart runner", s.ID)
				_ = s.FSM.Event(ctx, "begin_shutdown") // инициируем graceful‑stop → запуск заново можете добавить
			}
		}
	}
}
