package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
	"userapi/internal/domain"
	client2 "userapi/internal/postgresql/client"
)

type PgClient struct {
	client client2.Client
	logger *log.Logger
}

func (r *PgClient) InsertScenario(ctx context.Context, id string) error {
	tx, err := r.client.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	insertQuery := `
        INSERT INTO scenarios (id)
        VALUES ($1)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    `

	var insertedID string
	err = tx.QueryRow(ctx, insertQuery, id).Scan(&insertedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// запись уже есть — просто выходим без ошибки
			return nil
		}
		return fmt.Errorf("insert scenario: %w", err)
	}

	// если дошли до сюда — была успешная вставка → добавим в outbox
	outboxQuery := `
        INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
        VALUES ($1, $2, $3, $4)
    `

	payload := map[string]any{
		"id":     insertedID,
		"status": "inactive", // по умолчанию
	}
	payloadJSON, _ := json.Marshal(payload)

	_, err = tx.Exec(ctx, outboxQuery, "scenario", insertedID, "scenario.created", payloadJSON)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *PgClient) UpdateScenarioStatus(ctx context.Context, id string, newStatus string) error {
	tx, err := r.client.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `
        UPDATE scenarios 
        SET status = $2, updated_at = now()
        WHERE id = $1
        RETURNING id
    `

	var updatedID string
	err = tx.QueryRow(ctx, updateQuery, id, newStatus).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("scenario not found")
		}
		return fmt.Errorf("update scenario: %w", err)
	}

	outboxQuery := `
        INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
        VALUES ($1, $2, $3, $4)
    `
	payload := map[string]any{
		"id":     id,
		"status": newStatus,
	}
	payloadJSON, _ := json.Marshal(payload)

	_, err = tx.Exec(ctx, outboxQuery, "scenario", id, "scenario.status_updated", payloadJSON)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	return tx.Commit(ctx)
}

// TODO: Maybe add more variants
func (r *PgClient) ApplyKafkaEvent(ctx context.Context, evt domain.KafkaEvent) error {
	switch evt.EventType {

	case "scenario.status_updated":
		var data struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(evt.Payload, &data); err != nil {
			return fmt.Errorf("unmarshal scenario.status_updated: %w", err)
		}

		_, err := r.client.Exec(ctx,
			`UPDATE scenarios SET status = $1, updated_at = now() WHERE id = $2`,
			data.Status, data.ID,
		)
		return err

	case "scenario.created":
		var data struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(evt.Payload, &data); err != nil {
			return fmt.Errorf("unmarshal scenario.created: %w", err)
		}

		_, err := r.client.Exec(ctx,
			`
        INSERT INTO scenarios (id, status)
        VALUES ($1, $2)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    `,
			data.ID, data.Status,
		)
		return err

	// Maybe case "scenario.deleted": ...
	default:
		log.Printf("Unhandled event type: %s", evt.EventType)
		return nil
	}
}

func NewPgClient(client client2.Client, logger *log.Logger) *PgClient {
	return &PgClient{client, logger}
}
