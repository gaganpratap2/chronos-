
#!/bin/bash
set -Eeuo pipefail    #makes the script stop when a command fails.

echo "Initializing Chronos database..."

# Load the baseline schema (already includes migrations 001–010).
psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    --file /schema/chronos_schema.sql

# Apply migrations 011–017 in order.
for migration in /migrations/01[1-7]_*.sql; do
    echo "Applying migration: $(basename "$migration")"

    psql \
        --username "$POSTGRES_USER" \
        --dbname "$POSTGRES_DB" \
        --set ON_ERROR_STOP=1 \
        --file "$migration"
done

echo "Chronos database initialization completed."
