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
	runner     map[string]*orchestrator.Scenario
	enterState func(ctx context.Context, id string, dst string)
	mu         sync.RWMutex
}

//var runnerPool ScenarioPool
//
//func init() {
//	runnerPool = ScenarioPool{
//		runner: make(map[string]*orchestrator.Scenario),
//	}
//}

func NewScenarioPool(enterStateFunc func(ctx context.Context, id string, dst string)) *ScenarioPool {
	return &ScenarioPool{
		runner:     make(map[string]*orchestrator.Scenario),
		enterState: enterStateFunc,
	}
}

func (r *ScenarioPool) ScenarioExists(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.runner[id]
	return ok
}

func (r *ScenarioPool) RunScenario(ctx context.Context, id string) error {

	r.mu.Lock()
	defer r.mu.Unlock()

	sc, ok := r.runner[id]
	if ok {
		if sc.FSM.Current() != orchestrator.StInactive {
			return nil
		}
	} else {
		sc = orchestrator.NewScenario(id, r.enterState)
	}
	// ---------- запуск ----------
	if err := sc.FSM.Event(ctx, "begin_startup"); err != nil {
		log.Fatalf("[orchestrator] runners[RunScenario] startup error: %v", err)
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
		log.Fatalf("[orchestrator] runners[StopScenario] shutdown error: %v", err)
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
