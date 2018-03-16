CREATE VIEW same_view_1 AS SELECT prod_id, title, price FROM products WHERE common_prod_id > 0;

CREATE VIEW same_view_2 AS SELECT orderdate, COUNT(*) AS total_day FROM orders GROUP BY orderdate;

ALTER VIEW same_view_2 SET (security_barrier=on);

CREATE VIEW same_view_3 AS SELECT orderdate, COUNT(*) AS total_day FROM orders GROUP BY orderdate;

-- CREATE VIEW same_view_4 AS SELECT * FROM same_view_3;
