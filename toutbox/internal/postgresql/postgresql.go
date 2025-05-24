package postgresql

import (
	"context"
	"log"
	"toutbox/internal/domain"
	client2 "toutbox/internal/postgresql/client"
)

type PgClient struct {
	client client2.Client
	logger *log.Logger
}

func (pg *PgClient) GetUnprocessedOutboxEvents(ctx context.Context, limit int) ([]domain.KafkaEvent, error) {
	rows, err := pg.client.Query(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload
		FROM outbox
		WHERE processed = false
		ORDER BY created_at ASC
		LIMIT $1`, limit)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.KafkaEvent
	for rows.Next() {
		var evt domain.KafkaEvent
		var payloadBytes []byte
		if err := rows.Scan(&evt.ID, &evt.AggregateType, &evt.AggregateID, &evt.EventType, &payloadBytes); err != nil {
			return nil, err
		}
		evt.Payload = payloadBytes
		events = append(events, evt)
	}
	return events, nil
}

func (pg *PgClient) MarkEventProcessed(ctx context.Context, id string) error {
	_, err := pg.client.Exec(ctx,
		`UPDATE outbox 
		 SET processed = true, 
		     processed_at = now()
		 WHERE id = $1`, id)
	return err
}

func NewPgClient(client client2.Client, logger *log.Logger) *PgClient {
	return &PgClient{client, logger}
}
