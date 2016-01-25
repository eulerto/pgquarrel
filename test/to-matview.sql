CREATE MATERIALIZED VIEW same_matview_1 WITH (autovacuum_analyze_scale_factor=0.08,autovacuum_vacuum_scale_factor=0.25,autovacuum_vacuum_threshold=1234) AS SELECT prod_id, title, price FROM products WHERE common_prod_id > 0;

CREATE MATERIALIZED VIEW same_matview_2 AS SELECT orderdate, COUNT(*) AS total_day FROM orders GROUP BY orderdate;

