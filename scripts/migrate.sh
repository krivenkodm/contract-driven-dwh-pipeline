#!/usr/bin/env bash

set -euo pipefail


MIGRATIONS_DIR="${MIGRATIONS_DIR:-sql/migrations}"

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-contract_dwh_postgres}"
POSTGRES_USER="${POSTGRES_USER:-dwh}"
POSTGRES_DB="${POSTGRES_DB:-dwh}"

PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"


if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="python3"
fi


if [[ ! -d "${MIGRATIONS_DIR}" ]]; then
    echo "Migrations directory not found: ${MIGRATIONS_DIR}"
    exit 1
fi


psql_command=(
    docker exec -i
    "${POSTGRES_CONTAINER}"
    psql
    -X
    -U "${POSTGRES_USER}"
    -d "${POSTGRES_DB}"
    -v ON_ERROR_STOP=1
)


echo "Creating migration registry if necessary"

"${psql_command[@]}" <<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
    version bigint PRIMARY KEY,
    filename varchar NOT NULL,
    checksum varchar(64) NOT NULL,
    applied_dttm timestamp NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);
SQL


shopt -s nullglob

migration_files=(
    "${MIGRATIONS_DIR}"/*.sql
)

shopt -u nullglob


if [[ ${#migration_files[@]} -eq 0 ]]; then
    echo "No migration files found"
    exit 0
fi


echo "Checking ${#migration_files[@]} migration file(s)"


for migration_file in "${migration_files[@]}"; do
    filename="$(basename "${migration_file}")"

    if [[ ! "${filename}" =~ ^([0-9]+)_.+\.sql$ ]]; then
        echo "Invalid migration filename: ${filename}"
        echo "Expected format: 001_description.sql"
        exit 1
    fi

    version_text="${BASH_REMATCH[1]}"

    # Убираем ведущие нули:
    # 001 -> 1, 010 -> 10.
    version=$((10#${version_text}))

    checksum="$(
        "${PYTHON_BIN}" -c '
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
print(hashlib.sha256(path.read_bytes()).hexdigest())
' "${migration_file}"
    )"

    migration_record="$(
        "${psql_command[@]}" \
            -A \
            -t \
            -F '|' \
            -c "
                SELECT
                    filename,
                    checksum
                FROM schema_migrations
                WHERE version = ${version};
            "
    )"

    if [[ -n "${migration_record}" ]]; then
        applied_filename="${migration_record%%|*}"
        applied_checksum="${migration_record#*|}"

        if [[ "${applied_filename}" != "${filename}" ]]; then
            echo "Migration version conflict: ${version}"
            echo "Database filename: ${applied_filename}"
            echo "Local filename:    ${filename}"
            exit 1
        fi

        if [[ "${applied_checksum}" != "${checksum}" ]]; then
            echo "Applied migration was modified: ${filename}"
            echo "Database checksum: ${applied_checksum}"
            echo "Local checksum:    ${checksum}"
            exit 1
        fi

        echo "SKIP  ${filename}"
        continue
    fi

    echo "APPLY ${filename}"

    {
        echo "BEGIN;"

        cat "${migration_file}"

        cat <<'SQL'

INSERT INTO schema_migrations (
    version,
    filename,
    checksum,
    applied_dttm
)
VALUES (
    :migration_version,
    :'migration_filename',
    :'migration_checksum',
    CURRENT_TIMESTAMP
);

COMMIT;
SQL
    } | "${psql_command[@]}" \
        -v migration_version="${version}" \
        -v migration_filename="${filename}" \
        -v migration_checksum="${checksum}"

    echo "DONE  ${filename}"
done


echo
echo "Applied migrations:"

"${psql_command[@]}" -c "
    SELECT
        version,
        filename,
        applied_dttm
    FROM schema_migrations
    ORDER BY version;
"