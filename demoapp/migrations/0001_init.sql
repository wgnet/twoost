-- -*- mode: sql; sql-product: postgres; -*-

CREATE TABLE events (
  kind text,
  description text,
  payload bytea,
  created timestamp DEFAULT current_timestamp,
);
