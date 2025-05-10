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
	exit   chan struct{}
	active bool
	mutex  sync.Mutex
}

func NewRunner() *Runner {
	return &Runner{
		exit:   make(chan struct{}),
		active: false,
	}
}

func (r *Runner) Start(ctx context.Context, jobName string) bool {
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
	}(jobName)

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
	}(jobName)
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
