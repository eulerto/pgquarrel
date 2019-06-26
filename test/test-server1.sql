SET client_min_messages TO WARNING;

DROP DATABASE IF EXISTS quarrel1;
CREATE DATABASE quarrel1;

DROP ROLE IF EXISTS same_role_1;
DROP ROLE IF EXISTS same_role_2;
DROP ROLE IF EXISTS same_role_3;
CREATE ROLE same_role_1;
CREATE ROLE same_role_2;
CREATE ROLE same_role_3;

RESET client_min_messages;

\c quarrel1

-- fdw tests
CREATE EXTENSION postgres_fdw;
CREATE EXTENSION file_fdw;

\i dellstore.sql

\i from-table.sql

\i from-index.sql

\i from-sequence.sql

\i from-domain.sql

\i from-function.sql

\i from-extension.sql

\i from-language.sql

\i from-schema.sql

--\i from-eventtrigger.sql

\i from-cast.sql

\i from-collation.sql

\i from-view.sql

\i from-matview.sql

\i from-fdw.sql

\i from-server.sql

\i from-statistics.sql

\i from-enum-type.sql

--\i from-user-mapping.sql
