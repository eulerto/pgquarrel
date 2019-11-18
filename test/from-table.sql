CREATE TABLE same_table_1 (
	a integer not null,
	b text not null,
	c varchar(40),
	PRIMARY KEY(a)
);

ALTER TABLE same_table_1 ALTER COLUMN b SET (n_distinct=7);
COMMENT ON TABLE same_table_1 IS 'this is comment for table same_table_1';
COMMENT ON COLUMN same_table_1.b IS 'this is comment for column same_table_1.b';

CREATE TABLE same_table_2 (
	a integer not null,
	b text,
	PRIMARY KEY(a)
);
COMMENT ON TABLE same_table_2 IS 'this is comment for table same_table_2';
COMMENT ON COLUMN same_table_2.b IS 'this is comment for column same_table_2.b';

CREATE TABLE same_table_3 (
	a integer not null,
	b text,
	c numeric(5,2),
	PRIMARY KEY(a)
);

ALTER TABLE same_table_3 ALTER COLUMN c SET (n_distinct=5,n_distinct_inherited=10);

CREATE TABLE same_table_4 (
	a integer not null,
	b text,
	PRIMARY KEY(a)
) WITH (fillfactor=50,autovacuum_enabled=off);

CREATE TABLE from_table1 (
	a integer NOT NULL,
	b varchar(30) NOT NULL,
	PRIMARY KEY(a)
);

-- typed table
CREATE TYPE same_type_1 AS (
	foo varchar(30),
	bar varchar(10),
	baz integer
);

CREATE TABLE same_table_5 OF same_type_1;

CREATE TABLE same_table_6 (
	foo varchar(30),
	bar varchar(10),
	baz integer
);

-- reloptions
ALTER TABLE same_table_1 SET (autovacuum_enabled = off, autovacuum_vacuum_cost_delay = 25, autovacuum_analyze_threshold = 1234);
ALTER TABLE same_table_3 SET (autovacuum_analyze_scale_factor = 0.36, autovacuum_vacuum_scale_factor = 0.44);

-- statistics target
ALTER TABLE same_table_1 ALTER COLUMN b SET STATISTICS 90;
ALTER TABLE same_table_1 ALTER COLUMN c SET STATISTICS 33;

-- storage
ALTER TABLE same_table_1 ALTER COLUMN b SET STORAGE EXTERNAL;

-- replica identity
ALTER TABLE same_table_1 REPLICA IDENTITY FULL;
ALTER TABLE same_table_2 REPLICA IDENTITY NOTHING;

-- privileges
GRANT SELECT, INSERT, UPDATE ON TABLE same_table_1 TO same_role_1;
GRANT SELECT(a, b), INSERT (a, b), UPDATE (a, b) ON TABLE same_table_3 TO same_role_2;

-- partition
-- XXX test unsupported feature (regular <-> partitioned)
--CREATE TABLE same_table_7 (
--a serial,
--b varchar(20) NOT NULL
--) PARTITION BY RANGE (a);

--CREATE TABLE same_table_8 (
--a serial,
--b varchar(20) NOT NULL
--);

CREATE TABLE same_cities (
abbrev char(2) not null,
description varchar(80) not null,
id serial
) PARTITION BY LIST (abbrev);

CREATE TABLE same_cities_north PARTITION OF same_cities FOR VALUES IN ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') PARTITION BY LIST (abbrev);

CREATE TABLE same_cities_southest PARTITION OF same_cities FOR VALUES IN ('ES', 'MG', 'RJ', 'SP') PARTITION BY LIST (abbrev);

CREATE TABLE same_cities_south PARTITION OF same_cities FOR VALUES IN ('PR', 'RS', 'SC') PARTITION BY LIST (abbrev);

CREATE TABLE same_cities_pr PARTITION OF same_cities_south (primary key(id)) FOR VALUES IN ('PR');
CREATE TABLE same_cities_rs PARTITION OF same_cities_south (primary key(id)) FOR VALUES IN ('RS');
CREATE TABLE same_cities_sc PARTITION OF same_cities_south (primary key(id)) FOR VALUES IN ('SC');

CREATE TABLE same_cities_to PARTITION OF same_cities_north (primary key(id)) FOR VALUES IN ('TO');

--
-- foreign table
--
CREATE FOREIGN TABLE from_foreign_table_1 (
	a integer not null,
	b text,
	c numeric(5,2)
) SERVER server1;

CREATE FOREIGN TABLE same_foreign_table_1 (
	a integer not null,
	b text
) SERVER server1;

CREATE FOREIGN TABLE same_foreign_table_2 (
	a integer not null,
	b text,
	d boolean
) SERVER server1;

COMMENT ON FOREIGN TABLE same_foreign_table_1 IS 'this is a foreign table same_foreign_table_1';
