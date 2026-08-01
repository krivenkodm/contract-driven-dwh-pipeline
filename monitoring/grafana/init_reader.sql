\set ON_ERROR_STOP on

\getenv grafana_reader_user GRAFANA_READER_USER
\getenv grafana_reader_password GRAFANA_READER_PASSWORD

\if :{?grafana_reader_user}
\else
    \echo 'GRAFANA_READER_USER is required'
    \quit
\endif

\if :{?grafana_reader_password}
\else
    \echo 'GRAFANA_READER_PASSWORD is required'
    \quit
\endif

select format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'grafana_reader_user',
    :'grafana_reader_password'
)
where not exists (
    select 1
    from pg_roles
    where rolname = :'grafana_reader_user'
) \gexec

select format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 5',
    :'grafana_reader_user',
    :'grafana_reader_password'
) \gexec

select format(
    'ALTER ROLE %I SET default_transaction_read_only = on',
    :'grafana_reader_user'
) \gexec

select format(
    'GRANT CONNECT ON DATABASE %I TO %I',
    current_database(),
    :'grafana_reader_user'
) \gexec

select format(
    'GRANT USAGE ON SCHEMA dbt_monitoring TO %I',
    :'grafana_reader_user'
) \gexec

select format(
    'GRANT SELECT ON ALL TABLES IN SCHEMA dbt_monitoring TO %I',
    :'grafana_reader_user'
) \gexec

select format(
    'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA dbt_monitoring GRANT SELECT ON TABLES TO %I',
    current_user,
    :'grafana_reader_user'
) \gexec
