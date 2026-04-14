-- Bootstrap helper for the talos-aor Postgres container.
-- Runs BEFORE 002_init_aor.sql alphabetically.
-- The container's POSTGRES_DB creates `synapse`. This file adds `talos_aor`.
CREATE DATABASE talos_aor;
GRANT ALL PRIVILEGES ON DATABASE talos_aor TO CURRENT_USER;
