package client

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
	"log"
	"time"
	"toutbox/internal/utils"
)

type Client interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

func NewClient(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf(
		"postgresql://%s:%s@%s:%s/%s",
		viper.GetString("postgresql.DATABASE_USER"),
		viper.GetString("postgresql.DATABASE_PASSWORD"),
		viper.GetString("postgresql.DATABASE_HOST"),
		viper.GetString("postgresql.DATABASE_PORT"),
		viper.GetString("postgresql.DATABASE_NAME"),
	)
	var pool *pgxpool.Pool
	var err error
	maxAttempts := viper.GetInt("postgresql.max_attempts")

	err = utils.DoWithTries(func() error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		pool, err = pgxpool.New(ctx, dsn)
		return err
	}, maxAttempts, 5*time.Second)

	if err != nil {
		log.Fatalf("[outbox] Failed to connect to PostgreSQL after %d attempts: %v", maxAttempts, err)
		return nil, err
	}

	log.Printf("[outbox] Connected to PostgreSQL in less than %d attempts", maxAttempts)

	return pool, nil
}
