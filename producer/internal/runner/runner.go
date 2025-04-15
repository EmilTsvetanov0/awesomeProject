package runner

import (
	"awesomeProject/internal/kafka"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"
)

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

func (r *Runner) Start(jobName string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.active {
		return false
	}
	r.active = true
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
