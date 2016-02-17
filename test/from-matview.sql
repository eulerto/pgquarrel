CREATE MATERIALIZED VIEW same_matview_1 WITH (fillfactor=65,autovacuum_enabled=off) AS SELECT prod_id, title, price FROM products WHERE common_prod_id > 0;

CREATE MATERIALIZED VIEW same_matview_2 WITH (fillfactor=88) AS SELECT orderdate, COUNT(*) AS total_day FROM orders GROUP BY orderdate;

-- statistics target
ALTER MATERIALIZED VIEW same_matview_1 ALTER COLUMN prod_id SET STATISTICS 80;
ALTER MATERIALIZED VIEW same_matview_1 ALTER COLUMN price SET STATISTICS 21;

-- storage
ALTER MATERIALIZED VIEW same_matview_1 ALTER COLUMN title SET STORAGE EXTERNAL;
