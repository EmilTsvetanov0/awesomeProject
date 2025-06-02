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
	query := `
        SELECT status
        FROM scenarios
        WHERE id = $1
    `

	var status string
	err := r.client.QueryRow(ctx, query, id).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "unknown", fmt.Errorf("scenario not found")
		}
		return "unknown", fmt.Errorf("get scenario status: %w", err)
	}

	return status, nil
}

func (r *PgClient) GetPredictions(ctx context.Context, scenario_id string) ([]domain.Prediction, error) {
	query := `
		SELECT scenario_id, class, created_at
		FROM images
		WHERE scenario_id = $1
		ORDER BY created_at DESC
	`

	var predictions []domain.Prediction

	rows, err := r.client.Query(ctx, query, scenario_id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return predictions, nil
		}
		return predictions, fmt.Errorf("get predictions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var prediction domain.Prediction

		if err = rows.Scan(&prediction.ScenarioId, &prediction.Class, &prediction.CreatedAt); err != nil {
			return nil, fmt.Errorf("get predictions: %w", err)
		}

		predictions = append(predictions, prediction)

	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get predictions: %w", err)
	}

	return predictions, nil
}

// TODO: Maybe add more variants
func (r *PgClient) ApplyKafkaEvent(ctx context.Context, evt domain.KafkaEvent) error {
	switch evt.AggregateType {
	case "image":
		switch evt.EventType {
		case "created":
			var data struct {
				ScenarioId string `json:"scenario_id"`
				Class      string `json:"class"`
			}
			if err := json.Unmarshal(evt.Payload, &data); err != nil {
				return fmt.Errorf("unmarshal image.created: %w", err)
			}

			_, err := r.client.Exec(ctx,
				`
					INSERT INTO images (scenario_id, class)
					VALUES ($1, $2)
					`,
				data.ScenarioId,
				data.Class,
			)
			return err
		}
	case "scenario":
		switch evt.EventType {

		case "status_updated":
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

		case "created":
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
		}
	}
	log.Printf("Unhandled event type: %s", evt.EventType)
	return nil
}

func NewPgClient(client client2.Client, logger *log.Logger) *PgClient {
	return &PgClient{client, logger}
}
