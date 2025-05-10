package runners

import (
	"context"
	"fmt"
	"log"
	orchestrator "orchestrator/internal/scenario"
	"sync"
)

var ScenarioNotFoundErr = fmt.Errorf("scenario not found")

type ScenarioPool struct {
	runner map[string]*orchestrator.Scenario
	mu     sync.Mutex
}

//var runnerPool ScenarioPool
//
//func init() {
//	runnerPool = ScenarioPool{
//		runner: make(map[string]*orchestrator.Scenario),
//	}
//}

func NewScenarioPool() *ScenarioPool {
	return &ScenarioPool{
		runner: make(map[string]*orchestrator.Scenario),
	}
}

func (r *ScenarioPool) RunScenario(ctx context.Context, id string) error {

	r.mu.Lock()
	defer r.mu.Unlock()

	sc, ok := r.runner[id]
	if ok && sc.FSM.Current() != orchestrator.StInactive {
		return nil
	}

	sc = orchestrator.NewScenario(id)
	// ---------- запуск ----------
	if err := sc.FSM.Event(ctx, "begin_startup"); err != nil {
		log.Fatalf("startup error: %v", err)
	}

	return nil
}

func (r *ScenarioPool) StopScenario(ctx context.Context, id string) error {

	r.mu.Lock()
	defer r.mu.Unlock()

	sc, ok := r.runner[id]
	if !ok || sc.FSM.Current() == orchestrator.StInactive {
		return nil
	}
	// ---------- запуск ----------
	if err := sc.FSM.Event(ctx, "begin_shutdown"); err != nil {
		log.Fatalf("shutdown error: %v", err)
	}

	return nil
}

func (r *ScenarioPool) AcceptHeartbeat(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	sc, ok := r.runner[id]
	if !ok {
		return ScenarioNotFoundErr
	}

	sc.AcceptHeartbeat()
	return nil
}
