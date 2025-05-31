package runners

import (
	"context"
	"fmt"
	"log"
	orchestrator "orchestrator/internal/scenario"
	"sync"
)

var ScenarioNotFoundErr = fmt.Errorf("scenario not found")
var ScenarioNotActiveErr = fmt.Errorf("scenario not active")

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

	log.Println("[orchestrator] [runners.RunScenario] Running scenario", id)

	r.mu.Lock()
	defer r.mu.Unlock()

	sc, ok := r.runner[id]
	if ok {
		sc.Reset()
		if sc.FSM.Current() != orchestrator.StInactive {
			log.Printf("[orchestrator] [runners.RunScenario] Scenario %s is already in state %s", id, sc.FSM.Current())
			return nil
		}
	} else {
		log.Printf("[orchestrator] [runners.RunScenario] Scenario %v does not exist, creating scenario", id)
		sc = orchestrator.NewScenario(id, r.enterState)
		r.runner[id] = sc
	}
	// ---------- запуск ----------
	log.Printf("[orchestrator] [runners.RunScenario] Running scenario %v", id)
	if err := sc.FSM.Event(context.Background(), "begin_startup"); err != nil {
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
	// Опасно передавать контекст из аргумента
	if err := sc.FSM.Event(context.Background(), "begin_shutdown"); err != nil {
		return fmt.Errorf("[orchestrator] runners[StopScenario] shutdown error: %v", err)
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

	if sc.FSM.Current() != orchestrator.StActive && sc.FSM.Current() != orchestrator.StInStartupProcessing {
		log.Printf("[orchestrator] [runners.AcceptHeartbeat] Scenario %s is in state %s", id, sc.FSM.Current())
		return ScenarioNotActiveErr
	}

	sc.AcceptHeartbeat()
	return nil
}
