-- The integration suite's role and database. The role is deliberately not a
-- superuser: row-level security does not apply to one, so a suite run as
-- postgres would pass without proving anything. CI runs this same file.
CREATE ROLE sluice LOGIN PASSWORD 'sluicepass' NOSUPERUSER;
CREATE DATABASE sluice OWNER sluice;
