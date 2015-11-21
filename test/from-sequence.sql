CREATE SEQUENCE from_sequence_1;

CREATE SEQUENCE same_sequence_1;

COMMENT ON SEQUENCE same_sequence_1 IS 'this is comment for same_sequence_1';

GRANT USAGE ON SEQUENCE same_sequence_1 TO same_role_1;
GRANT ALL PRIVILEGES ON SEQUENCE same_sequence_1 TO same_role_2;
