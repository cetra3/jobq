DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'Status') THEN
        CREATE TYPE "Status" as enum ('Queued', 'Processing', 'Completed', 'Failed');
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS jobq (
    id serial primary key,
    name text,
    uuid uuid,
    params jsonb,
    priority int,
    status "Status",
    "time" timestamp with time zone,
    duration double precision,
    error text
);
