package runners

import (
	"context"
	"fmt"
	"log"
	"orchestrator/internal/domain"
	"orchestrator/internal/postgresql"
	orchestrator "orchestrator/internal/scenario"
	"sync"
)

var ScenarioNotFoundErr = fmt.Errorf("scenario not found")
var ScenarioNotActiveErr = fmt.Errorf("scenario not active")

type ScenarioPool struct {
	runners    map[string]*orchestrator.Scenario
	workers    map[string]chan *domain.RunnerMsg
	pg         *postgresql.PgClient
	enterState func(ctx context.Context, id string, dst string)
	mu         sync.RWMutex
	rs         orchestrator.RunnerService
	wg         sync.WaitGroup
}

func NewScenarioPool(pgClient *postgresql.PgClient, enterStateFunc func(ctx context.Context, id string, dst string), runnerService orchestrator.RunnerService) *ScenarioPool {
	return &ScenarioPool{
		runners:    make(map[string]*orchestrator.Scenario),
		workers:    make(map[string]chan *domain.RunnerMsg),
		enterState: enterStateFunc,
		pg:         pgClient,
		rs:         runnerService,
	}
}

func (r *ScenarioPool) FinishWorkers() {
	defer func() {
		if re := recover(); re != nil {
			log.Printf("[orchestrator] ошибка при закрытии канала - %v", re)
		}
	}()

	r.mu.RLock()
	defer r.mu.RUnlock()

	for id, channel := range r.workers {
		if channel == nil {
			log.Printf("[orchestrator] Channel %s is nil", id)
			continue
		}

		select {
		case _, ok := <-channel:
			if ok {
				close(channel)
			} else {
				log.Printf("[orchestrator] Channel %s is closed ", id)
				continue
			}
		default:
			close(channel)
		}
	}

	log.Print("[orchestrator] Successfully closed workers")
}

func (r *ScenarioPool) WaitForWorkers() {
	log.Print("[orchestrator] Waiting for workers")
	r.wg.Wait()
	log.Print("[orchestrator] All workers finished")
}

func (r *ScenarioPool) HandleScenario(ctx context.Context, runnerMsg *domain.RunnerMsg) {
	r.mu.Lock()
	defer r.mu.Unlock()

	worker, ok := r.workers[runnerMsg.Id]
	if !ok {
		worker = make(chan *domain.RunnerMsg, 10)
		r.workers[runnerMsg.Id] = worker
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.ScenarioWorker(ctx, runnerMsg.Id, worker)
		}()
	}

	worker <- runnerMsg
}

func (r *ScenarioPool) ScenarioWorker(ctx context.Context, id string, ch <-chan *domain.RunnerMsg) {
	for msg := range ch {
		if msg.Action == "start" {
			log.Printf("Starting scenario %s", msg.Id)
			if err := r.RunScenario(ctx, msg.Id); err != nil {
				log.Printf("[orchestrator] Error running scenario: %v", err)
			}
		} else if msg.Action == "stop" {
			if err := r.StopScenario(ctx, msg.Id); err != nil {
				log.Printf("[orchestrator] Error stopping scenario: %v", err)
			}
		} else {
			log.Printf("[orchestrator] Skipping scenario %s, command \"%s\" is not recognized", msg.Id, msg.Action)
		}
	}

	r.mu.Lock()
	delete(r.workers, id)
	r.mu.Unlock()
}

func (r *ScenarioPool) RunScenario(ctx context.Context, id string) error {

	log.Println("[orchestrator] [runners.RunScenario] Running scenario", id)

	r.mu.Lock()
	defer r.mu.Unlock()

	sc, ok := r.runners[id]

	if ok {
		if sc.FSM.Current() != orchestrator.StInactive {
			log.Printf("[orchestrator] [runners.RunScenario] Scenario %s is already in state %s", id, sc.FSM.Current())
			return nil
		}
		sc.Reset()
	} else {
		log.Printf("[orchestrator] [runners.RunScenario] Scenario %v does not exist, creating scenario", id)
		err := r.pg.InsertScenario(ctx, id)
		if err != nil {
			return err
		}

		sc = orchestrator.NewScenario(id, r.enterState, r.rs)
		r.runners[id] = sc
	}
	// ---------- запуск ----------
	log.Printf("[orchestrator] [runners.RunScenario] Running scenario %v", id)
	if err := sc.FSM.Event(context.Background(), "begin_startup"); err != nil {
		return fmt.Errorf("[orchestrator] runners[RunScenario] startup error: %v", err)
	}

	return nil
}

func (r *ScenarioPool) StopScenario(ctx context.Context, id string) error {

	r.mu.RLock()
	sc, ok := r.runners[id]
	r.mu.RUnlock()

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
	r.mu.RLock()
	sc, ok := r.runners[id]
	r.mu.RUnlock()

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
