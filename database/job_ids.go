package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func NextJobID(
	ctx context.Context,
	tx pgx.Tx,
) (int, error) {
	var jobID int

	err := tx.QueryRow(
		ctx,
		`SELECT nextval('jobs_id_seq')`,
	).Scan(&jobID)

	if err != nil {
		return 0, fmt.Errorf(
			"generate job ID: %w",
			err,
		)
	}

	return jobID, nil
}
