CREATE TABLE same_table_1 (
	a integer not null,
	b text not null,
	c varchar(40),
	PRIMARY KEY(a)
);

ALTER TABLE same_table_1 ALTER COLUMN b SET (n_distinct=7);

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
