-- Set up anon in postgres
ALTER SYSTEM SET session_preload_libraries = 'anon';
SELECT pg_reload_conf();
CREATE EXTENSION IF NOT EXISTS anon CASCADE;