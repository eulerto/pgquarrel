CREATE SERVER server1 TYPE 'foo' VERSION '9.4' FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host 'localhost', port '9901', dbname 'postgres');
CREATE SERVER server2 VERSION '9.4' FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host 'localhost', port '9901');
