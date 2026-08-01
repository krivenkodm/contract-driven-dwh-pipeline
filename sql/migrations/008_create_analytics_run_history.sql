CREATE TABLE analytics_run_history (
    run_id uuid PRIMARY KEY,
    trigger_type varchar(32) NOT NULL,
    status varchar(16) NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    duration_seconds double precision,
    freshness_status varchar(16) NOT NULL DEFAULT 'not_run',
    build_status varchar(16) NOT NULL DEFAULT 'not_run',
    dbt_invocation_id varchar,
    total_nodes integer NOT NULL DEFAULT 0,
    successful_nodes integer NOT NULL DEFAULT 0,
    warned_nodes integer NOT NULL DEFAULT 0,
    failed_nodes integer NOT NULL DEFAULT 0,
    result_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text,

    CONSTRAINT ck_analytics_run_status
        CHECK (status IN ('running', 'success', 'warning', 'error')),

    CONSTRAINT ck_analytics_freshness_status
        CHECK (freshness_status IN ('not_run', 'pass', 'warn', 'error')),

    CONSTRAINT ck_analytics_build_status
        CHECK (build_status IN ('not_run', 'success', 'error')),

    CONSTRAINT ck_analytics_run_node_counts
        CHECK (
            total_nodes >= 0
            AND successful_nodes >= 0
            AND warned_nodes >= 0
            AND failed_nodes >= 0
        )
);

CREATE INDEX idx_analytics_run_history_started_at
    ON analytics_run_history (started_at DESC);

CREATE INDEX idx_analytics_run_history_status
    ON analytics_run_history (status, started_at DESC);

COMMENT ON TABLE analytics_run_history IS
    'Observed dbt freshness and build executions.';
