CREATE SERVER server1 TYPE 'foo' VERSION '9.5' FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host 'localhost', port '9902');
CREATE SERVER server3 FOREIGN DATA WRAPPER file_fdw;
