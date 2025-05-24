package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"producer/internal/cconfig"
	"producer/internal/kafka"
	"strconv"
	"sync"
	"time"
)

var hbInterval = 3 * time.Second
var heartbeatURL = "http://localhost:8080/ping"

func init() {
	cconfig.InitConfig()

	hbInterval = time.Duration(viper.GetInt("runner.heartbeat_interval")) * time.Second
	heartbeatURL = "http://" + viper.GetString("runner.heartbeat_url")
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
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.active {
		return false
	}
	r.active = true

	// Здесь отправляем хартбиты

	// TODO: Поменять, потому что завершение через контекст не сработает (сработает, но хочу по-другому)
	go func(job string) {
		ticker := time.NewTicker(hbInterval)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				log.Println("Heartbeat stopped")
				return

			case <-ticker.C:
				payload := []byte(`{"id":"` + job + `","timestamp":` + strconv.FormatInt(time.Now().Unix(), 10) + `}`)

				resp, err := http.Post(heartbeatURL, "application/json", bytes.NewBuffer(payload))
				if err != nil {
					log.Println("Heartbeat send error:", err)
					continue
				}
				resp.Body.Close()
			}
		}
	}(r.jobName)

	// Здесь отправляем кадры (пока симуляция)
	go func(job string) {
		ticker := time.NewTicker(3 * time.Second)
		i := 0
		for {
			select {
			case <-r.exit:
				return
			case <-ticker.C:
				i++
				msg, err := json.Marshal(kafka.Notification{
					Title:     "message number " + strconv.Itoa(i) + " for job " + job,
					Timestamp: time.Now().Unix(),
				})
				if err != nil {
					log.Print("Unmarshalling error: ", err)
					r.mutex.Lock()
					r.active = false
					r.mutex.Unlock()
					return
				}
				kafka.PushNotificationToQueue("notifications", msg)
			}
		}
	}(r.jobName)
	return true
}

func (r *Runner) Stop() bool {
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
	return &Pool{}
}

func (p *Pool) Start(ctx context.Context, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, ok := p.pool[id]
	if !ok {
		r = NewRunner(id)
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
