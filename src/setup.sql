DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'Status') THEN
        CREATE TYPE "Status" as enum ('Queued', 'Processing', 'Completed', 'Failed');
    END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'Priority') THEN
        CREATE TYPE "Priority" as enum ('High', 'Normal', 'Low');
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS jobq (
    id bigserial primary key,
    name text,
    uuid uuid,
    params jsonb,
    priority "Priority",
    status "Status",
    "time" timestamp with time zone,
    duration double precision,
    error text
);

CREATE INDEX IF NOT EXISTS status_idx ON jobq (
    status
);

CREATE INDEX IF NOT EXISTS priority_idx ON jobq (
    priority
);