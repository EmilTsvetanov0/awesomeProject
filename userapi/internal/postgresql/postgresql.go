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

var ErrNoScenario = fmt.Errorf("scenario does not exist")

type PgClient struct {
	client client2.Client
	logger *log.Logger
}

func (r *PgClient) GetScenarioStatus(ctx context.Context, id string) (string, error) {
	updateQuery := `
        SELECT status
        FROM scenarios
        WHERE id = $1
    `

	var status string
	err := r.client.QueryRow(ctx, updateQuery, id).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "unknown", fmt.Errorf("scenario not found")
		}
		return "unknown", fmt.Errorf("get scenario status: %w", err)
	}

	return status, nil
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
