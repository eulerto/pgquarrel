SET client_min_messages TO ERROR;

CREATE PUBLICATION same_pub_1;
CREATE PUBLICATION same_pub_2;
CREATE PUBLICATION "same_PUB_3";

CREATE PUBLICATION from_pub_1;

ALTER PUBLICATION same_pub_1 ADD TABLE ONLY customers;

ALTER PUBLICATION same_pub_2 ADD TABLE ONLY customers;
ALTER PUBLICATION same_pub_2 ADD TABLE ONLY products;

ALTER PUBLICATION from_pub_1 ADD TABLE ONLY customers;

RESET client_min_messages;
