-- Create flowbyte_destination database (internal DB already created via POSTGRES_DB env)
SELECT 'CREATE DATABASE flowbyte_destination'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'flowbyte_destination'
)\gexec

GRANT ALL PRIVILEGES ON DATABASE flowbyte_destination TO flowbyte;
