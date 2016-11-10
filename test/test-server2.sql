SET client_min_messages TO WARNING;

DROP DATABASE IF EXISTS quarrel2;
CREATE DATABASE quarrel2;

DROP ROLE IF EXISTS same_role_1;
DROP ROLE IF EXISTS same_role_2;
DROP ROLE IF EXISTS same_role_3;
CREATE ROLE same_role_1;
CREATE ROLE same_role_2;
CREATE ROLE same_role_3;

RESET client_min_messages;

\c quarrel2

-- fdw tests
CREATE EXTENSION postgres_fdw;
CREATE EXTENSION file_fdw;

\i dellstore.sql

\i to-table.sql

\i to-index.sql

\i to-sequence.sql

\i to-domain.sql

\i to-function.sql

\i to-extension.sql

\i to-language.sql

\i to-schema.sql

--\i to-eventtrigger.sql

\i to-cast.sql

\i to-collation.sql

\i to-view.sql

\i to-matview.sql

\i to-fdw.sql

\i to-server.sql

--\i to-user-mapping.sql
