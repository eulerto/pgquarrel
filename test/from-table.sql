CREATE TABLE same_table_1 (
	a integer not null,
	b text not null,
	c varchar(40),
	PRIMARY KEY(a)
);

CREATE TABLE from_table1 (
	a integer NOT NULL,
	b varchar(30) NOT NULL,
	PRIMARY KEY(a)
);

-- statistics target
ALTER TABLE same_table_1 ALTER COLUMN b SET STATISTICS 90;
ALTER TABLE same_table_1 ALTER COLUMN c SET STATISTICS 33;

-- storage
ALTER TABLE same_table_1 ALTER COLUMN b SET STORAGE EXTERNAL;
