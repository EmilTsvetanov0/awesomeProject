package utils

import (
	"fmt"
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
	return fmt.Errorf("failed after %d attempts", attempts)
}
