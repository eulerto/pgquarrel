CREATE TABLE same_table_1 (
	a integer not null,
	b text not null,
	c varchar(40),
	d double precision,
	PRIMARY KEY(a)
);

ALTER TABLE same_table_1 ALTER COLUMN b SET (n_distinct=6);

CREATE TABLE same_table_2 (
	a integer not null,
	b text,
	PRIMARY KEY(a)
);

CREATE TABLE same_table_3 (
	a integer not null,
	b text,
	c numeric(5,2),
	PRIMARY KEY(a)
);

ALTER TABLE same_table_3 ALTER COLUMN c SET (n_distinct_inherited=8);

CREATE TABLE same_table_4 (
	a integer not null,
	b text,
	PRIMARY KEY(a)
) WITH (autovacuum_analyze_scale_factor=0.12,autovacuum_vacuum_cost_limit=234,autovacuum_vacuum_scale_factor=0.23);

CREATE TABLE to_table_1 (
	a integer not null,
	b varchar(30) not null,
	c text not null,
	d xml,
	e numeric(10,2),
	f text collate "en_US" default 'teste' not null,
	g varchar(100) default 'Euler Taveira',
	PRIMARY KEY(a)
);

CREATE TABLE to_table_2 (
	a serial,
	b varchar(40) not null,
	c integer not null,
	FOREIGN KEY(c) REFERENCES to_table_1(a) ON UPDATE RESTRICT ON DELETE RESTRICT,
	PRIMARY KEY(a)
);

CREATE TABLE to_table_3 (
	a integer,
	b text,
	c date,
	CHECK(c > '2010-01-01' AND c < '2014-12-31')
) WITH (autovacuum_enabled = off, autovacuum_vacuum_scale_factor = 0.4, autovacuum_analyze_scale_factor = 0.2);

--CREATE TABLE to_table_4 (
--) INHERITS (to_table_1);

-- typed table
CREATE TYPE same_type_1 AS (
	foo varchar(30),
	bar varchar(10),
	baz integer
);

CREATE TABLE same_table_5 (
	foo varchar(30),
	bar varchar(10),
	baz integer
);

CREATE TABLE same_table_6 OF same_type_1;

CREATE TABLE to_table_4 OF same_type_1;

-- reloptions
ALTER TABLE same_table_1 SET (autovacuum_enabled = off, autovacuum_vacuum_cost_delay = 13);
ALTER TABLE same_table_2 SET (autovacuum_vacuum_scale_factor = 0.44, autovacuum_analyze_scale_factor = 0.22);

-- statistics target
ALTER TABLE same_table_1 ALTER COLUMN c SET STATISTICS 25;
ALTER TABLE same_table_1 ALTER COLUMN d SET STATISTICS 44;
ALTER TABLE to_table_2 ALTER COLUMN c SET STATISTICS 123;

-- storage
ALTER TABLE to_table_3 ALTER COLUMN b SET STORAGE EXTERNAL;

-- replica identity
ALTER TABLE same_table_2 REPLICA IDENTITY USING INDEX same_table_2_pkey;

-- privileges
GRANT SELECT, DELETE ON TABLE same_table_1 TO same_role_1;
GRANT SELECT (a, b), INSERT (b) ON TABLE same_table_3 TO same_role_2;
GRANT SELECT ON TABLE to_table_1 TO same_role_1;
GRANT INSERT, UPDATE (b, c) ON TABLE to_table_2 TO same_role_2;
