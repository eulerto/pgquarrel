CREATE INDEX same_index_1 ON products (actor) WITH (fillfactor=65);

CREATE INDEX to_index_1 ON customers (lastname) WITH (fillfactor=40);

CREATE INDEX to_index_2 ON customers (city, region);

CREATE INDEX to_index_3 ON orders (orderdate DESC NULLS LAST);

CREATE INDEX to_index_4 ON products USING hash (title) WHERE title !~ 'default title';
