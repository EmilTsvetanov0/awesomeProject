package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/spf13/viper"
	"gocv.io/x/gocv"
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
var terminateURL = "http://localhost:8080/term"
var hbTries = 5
var frameSendInterval = 3000 * time.Millisecond

const videoFolder = "./files/"

func init() {
	cconfig.InitConfig()

	hbInterval = time.Duration(viper.GetInt("runner.heartbeat_interval")) * time.Second
	frameSendInterval = time.Duration(viper.GetInt("runner.frame_interval_millis")) * time.Millisecond
	heartbeatURL = "http://" + viper.GetString("runner.heartbeat_url")
	terminateURL = "http://" + viper.GetString("runner.terminate_url")
	hbTries = viper.GetInt("runner.heartbeat_tries")
}

type Runner struct {
	active  bool
	mutex   sync.RWMutex
	jobName string
}

func NewRunner(name string) *Runner {
	return &Runner{
		active:  false,
		jobName: name,
	}
}

func (r *Runner) isActive() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.active
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

	startFrames := make(chan struct{}, 1)

	go func(job string) {
		defer close(startFrames)
		started := false

		currentTries := 0
		ticker := time.NewTicker(hbInterval)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				log.Println("[producer] [Runner.Start] Heartbeat stopped")
				return

			case <-ticker.C:
				if !r.isActive() {
					log.Println("[producer] [Runner.Start] Stopping heartbeat sending due to runner inactivity")
					return
				}
				log.Printf("[producer] [Runner.Start] Sending heartbeat for %s", r.jobName)
				payload := []byte(`{"id":"` + job + `","timestamp":` + strconv.FormatInt(time.Now().Unix(), 10) + `}`)

				resp, err := http.Post(heartbeatURL, "application/json", bytes.NewBuffer(payload))
				log.Printf("[producer] [Runner.Start] Heartbeat response code: %d", resp.StatusCode)
				if err != nil || resp.StatusCode != http.StatusOK {
					if err != nil {
						log.Println("[producer] [Runner.Start] Heartbeat send error:", err)
					} else {
						log.Println("[producer] [Runner.Start] Heartbeat send error: status code:", resp.StatusCode)
					}
					currentTries++
					if currentTries >= hbTries {
						log.Println("[producer] [Runner.Start] Heartbeat tries limit reached, cancelling runner")
						r.Stop()
						err := resp.Body.Close()
						if err != nil {
							log.Printf("[producer] [Runner.Start] Close body response error: %s", err)
							return
						}
						return
					}
				} else {
					if !started {
						started = true
						startFrames <- struct{}{}
					}
					currentTries = 0
				}
				err = resp.Body.Close()
				if err != nil {
					log.Printf("[producer] [Runner.Start] Close body response error: %s", err)
					return
				}
			}
		}
	}(r.jobName)

	go func(job string) {

		select {
		case <-ctx.Done():
			log.Println("[producer] [Runner.Start] Frame sender: context canceled before start")
			return
		case <-startFrames:
			log.Println("[producer] [Runner.Start] Frame sender: starting after successful heartbeat")
		}

		video, err := gocv.VideoCaptureFile(videoFolder + job + ".mp4")
		if err != nil {
			log.Println("[producer] [Runner.Start] VideoCaptureFile error:", err)
			NotifyAboutStopping(job, err)
			r.Stop()
			return
		}
		defer video.Close()

		img := gocv.NewMat()
		defer img.Close()

		ticker := time.NewTicker(frameSendInterval)

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				log.Println("[producer] [Runner.Start] Frame sender stopped")
				return
			case <-ticker.C:
				if !r.isActive() {
					log.Println("[producer] [Runner.Start] Stopping frames sending due to runner inactivity")
					return
				}

				if !video.Read(&img) || img.Empty() {
					video.Close()
					video, err = gocv.VideoCaptureFile(job + ".mp4")
					if err != nil {
						log.Println("[producer] [Runner.Start] VideoCaptureFile error:", err)
						NotifyAboutStopping(job, err)
						r.Stop()
						return
					}
					continue
				}

				//frameData := img.ToBytes()

				buf, err := gocv.IMEncode(".jpg", img)
				if err != nil {
					log.Printf("[producer] [Runner.Start] failed to encode image: %v", err)
					r.Stop()
					return
				}
				frameData := buf.GetBytes()
				buf.Close()

				msg, err := json.Marshal(domain.VideoFrame{
					ScenarioId: r.jobName,
					Data:       frameData,
				})
				if err != nil {
					log.Print("[producer] [Runner.Start] Unmarshalling error: ", err)
					r.Stop()
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

func NotifyAboutStopping(job string, err error) {
	log.Println("[producer] [NotifyAboutStopping]", err)
	payload := []byte(`{"id":"` + job + `","timestamp":` + strconv.FormatInt(time.Now().Unix(), 10) + `, "error":"` + err.Error() + `"}`)

	resp, err := http.Post(terminateURL, "application/json", bytes.NewBuffer(payload))
	log.Printf("[producer] [NotifyAboutStopping] response code: %d", resp.StatusCode)
	if err != nil || resp.StatusCode != http.StatusOK {
		if err != nil {
			log.Println("Notification send error:", err)
		} else {
			log.Println("Notification send error: status code:", resp.StatusCode)
		}
	}
	err = resp.Body.Close()
	if err != nil {
		log.Printf("[producer] [NotifyAboutStopping] Close body response error: %s", err)
		return
	}
}
