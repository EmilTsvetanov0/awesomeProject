package utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func DoWithTries(fn func() error, attempts int, delay time.Duration) error {
	for attempts > 0 {
		if err := fn(); err == nil {
			return nil
		}
		attempts--
		time.Sleep(delay)
	}
	return fmt.Errorf("[outbox] failed after %d attempts", attempts)
}

func WaitForShutdownSignal(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	log.Println("[outbox] shutdown signal received")
	cancel()
}
