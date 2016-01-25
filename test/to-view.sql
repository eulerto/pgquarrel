CREATE VIEW same_view_1 AS SELECT prod_id, title, price FROM products WHERE common_prod_id > 0;

ALTER VIEW same_view_1 SET (security_barrier=on, check_option=cascaded);

CREATE VIEW same_view_2 AS SELECT orderdate, COUNT(*) AS total_day FROM orders GROUP BY orderdate;
