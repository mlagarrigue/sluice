-- The integration suite's role and database. The role is deliberately not a
-- superuser: row-level security does not apply to one, so a suite run as
-- postgres would pass without proving anything. CREATEROLE lets the SASLprep
-- test create the role whose password it checks; since PostgreSQL 16 it
-- reaches only the roles sluice creates. REPLICATION lets the replication
-- suite open a logical replication connection; it does not bypass row-level
-- security. CI runs this same file.
CREATE ROLE sluice LOGIN PASSWORD 'sluicepass' NOSUPERUSER CREATEROLE REPLICATION;
CREATE DATABASE sluice OWNER sluice;
