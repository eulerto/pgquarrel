CREATE TABLE same_table_1 (
	a integer not null,
	b text not null,
	c varchar(40),
	PRIMARY KEY(a)
);

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
