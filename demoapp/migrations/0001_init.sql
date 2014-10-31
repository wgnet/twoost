-- -*- mode: sql; sql-product: postgres; -*-

-- @ forward

CREATE TABLE events (
  id text primary key,
  payload bytea,
  created timestamp DEFAULT current_timestamp
);
