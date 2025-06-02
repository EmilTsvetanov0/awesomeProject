package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"producer/internal/cconfig"
	"producer/internal/domain"
	"producer/internal/kafka"
	"strconv"
	"sync"
	"time"
)

var hbInterval = 3 * time.Second
var heartbeatURL = "http://localhost:8080/ping"
var hbTries = 5

func init() {
	cconfig.InitConfig()

	hbInterval = time.Duration(viper.GetInt("runner.heartbeat_interval")) * time.Second
	heartbeatURL = "http://" + viper.GetString("runner.heartbeat_url")
	hbTries = viper.GetInt("runner.heartbeat_tries")
}

type Runner struct {
	exit    chan struct{}
	active  bool
	mutex   sync.Mutex
	jobName string
}

func NewRunner(name string) *Runner {
	return &Runner{
		exit:    make(chan struct{}),
		active:  false,
		jobName: name,
	}
}

func (r *Runner) Start(ctx context.Context) bool {
	log.Printf("[producer] [Runner.Start] Runner %s starting", r.jobName)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.active {
		return false
	}
	r.active = true

	// Здесь отправляем хартбиты

	// TODO: Поменять, потому что завершение через контекст не сработает (сработает, но хочу по-другому)
	// TODO: Не факт, что нужно менять
	// TODO: Нужно убрать моментальное начало обработки кадров, а запускать её только если хартбиты были приняты в первый раз
	go func(job string) {
		currentTries := 0
		ticker := time.NewTicker(hbInterval)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				log.Println("[producer] Heartbeat stopped")
				r.Stop()
				return

			case <-ticker.C:
				if !r.active {
					log.Println("[producer] Stopping heartbeat sending due to runner inactivity")
					return
				}
				log.Printf("[producer] [Runner.Start] Sending heartbeat for %s", r.jobName)
				payload := []byte(`{"id":"` + job + `","timestamp":` + strconv.FormatInt(time.Now().Unix(), 10) + `}`)

				resp, err := http.Post(heartbeatURL, "application/json", bytes.NewBuffer(payload))
				log.Printf("[producer] [Runner.Start] Heartbeat response code: %d", resp.StatusCode)
				if err != nil || resp.StatusCode != http.StatusOK {
					if err != nil {
						log.Println("Heartbeat send error:", err)
					} else {
						log.Println("Heartbeat send error: status code:", resp.StatusCode)
					}
					currentTries++
					if currentTries >= hbTries {
						log.Println("Heartbeat tries limit reached, cancelling runner")
						r.Stop()
						err := resp.Body.Close()
						if err != nil {
							log.Printf("[producer] [Runner.Start] Close body response error: %s", err)
							return
						}
						return
					}
				}
				err = resp.Body.Close()
				if err != nil {
					log.Printf("[producer] [Runner.Start] Close body response error: %s", err)
					return
				}
			}
		}
	}(r.jobName)

	// Здесь отправляем кадры (пока симуляция)
	// TODO: Подключить обработку кадров из видоса
	go func(job string) {
		ticker := time.NewTicker(3 * time.Second)
		i := 0
		for {
			select {
			case <-r.exit:
				return
			case <-ticker.C:
				i++
				//msg, err := json.Marshal(kafka.Notification{
				//	Title:     "message number " + strconv.Itoa(i) + " for job " + job,
				//	Timestamp: time.Now().Unix(),
				//})
				msg, err := json.Marshal(domain.VideoFrame{
					ScenarioId: r.jobName,
					Data:       []byte{},
				})
				if err != nil {
					log.Print("Unmarshalling error: ", err)
					r.mutex.Lock()
					r.active = false
					r.mutex.Unlock()
					return
				}
				kafka.PushImageToQueue(job, msg)
			}
		}
	}(r.jobName)
	return true
}

func (r *Runner) Stop() bool {
	log.Printf("[producer] [Runner.Stop] Runner %s stopping", r.jobName)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.active {
		return false
	}
	r.exit <- struct{}{}
	r.active = false
	return true
}

// Runner pool

type Pool struct {
	mu   sync.Mutex
	pool map[string]*Runner
}

func NewPool() *Pool {
	return &Pool{
		pool: make(map[string]*Runner),
	}
}

func (p *Pool) Start(ctx context.Context, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, ok := p.pool[id]
	if !ok {
		r = NewRunner(id)
		p.pool[id] = r
	}

	if !r.Start(ctx) {
		log.Println("[producer] Runner " + id + " already started")
	} else {
		log.Println("[producer] Runner " + id + " started")
	}
}

func (p *Pool) Stop(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, ok := p.pool[id]
	if !ok {
		log.Println("[producer] Runner " + id + " doesn't exist")
		return
	}

	if !r.Stop() {
		log.Println("[producer] Runner " + id + " already stopped")
	} else {
		log.Println("[producer] Runner " + id + " stopped")
	}
}
