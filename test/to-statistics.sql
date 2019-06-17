CREATE TABLE statistics_table_1 (
	a integer,
	b integer,
	c integer,
	d integer
);

CREATE STATISTICS same_statistics_1 (dependencies) ON a, b FROM statistics_table_1;

CREATE STATISTICS to_statistics_1 (dependencies) ON b, c FROM statistics_table_1;

COMMENT ON STATISTICS same_statistics_1 IS NULL;
COMMENT ON STATISTICS to_statistics_1 IS 'this is a statistics comment';

CREATE STATISTICS to_statistics_2 (dependencies) ON a, d FROM statistics_table_1;

COMMENT ON STATISTICS same_statistics_1 IS NULL;
COMMENT ON STATISTICS to_statistics_1 IS 'this is a statistics comment with ''';
