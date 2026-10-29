-- The integration suite's role and database. The role is deliberately not a
-- superuser: row-level security does not apply to one, so a suite run as
-- postgres would pass without proving anything. CREATEROLE lets the SASLprep
-- test create the role whose password it checks; since PostgreSQL 16 it
-- reaches only the roles sluice creates. REPLICATION lets the replication
-- suite open a logical replication connection; it does not bypass row-level
-- security. CI runs this same file.
CREATE ROLE sluice LOGIN PASSWORD 'sluicepass' NOSUPERUSER CREATEROLE REPLICATION;
CREATE DATABASE sluice OWNER sluice;

-- The replication suite marks a transaction with a replication origin, to
-- see the Origin message arrive. The pg_replication_origin_* functions are
-- revoked from PUBLIC; granting them to the role is enough, in the database
-- the suite runs in, whatever their signatures on this major.
\connect sluice
DO $$
DECLARE f regprocedure;
BEGIN
  FOR f IN SELECT oid::regprocedure FROM pg_proc WHERE proname LIKE 'pg\_replication\_origin\_%' LOOP
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO sluice', f);
  END LOOP;
END
$$;
