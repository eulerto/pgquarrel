CREATE SEQUENCE to_sequence_1;

CREATE SEQUENCE to_sequence_2;

CREATE SEQUENCE to_sequence_3 INCREMENT BY 10 START WITH 50 CYCLE;

CREATE SEQUENCE to_sequence_4 MINVALUE -10 MAXVALUE 15 CACHE 2 CYCLE;

CREATE SEQUENCE same_sequence_1;

COMMENT ON SEQUENCE to_sequence_2 IS 'this is comment for to_sequence_2';

GRANT USAGE ON SEQUENCE to_sequence_1 TO same_role_1, same_role_2, same_role_3;
GRANT ALL PRIVILEGES ON SEQUENCE same_sequence_1 TO same_role_1;
GRANT SELECT, USAGE ON SEQUENCE same_sequence_1 TO same_role_3;
