CREATE CAST (json AS character varying) WITHOUT FUNCTION AS IMPLICIT;

COMMENT ON CAST (json AS character varying) IS 'this is comment for cast json -> character varying';

CREATE CAST (text AS json) WITHOUT FUNCTION;

COMMENT ON CAST (text AS json) IS 'this is comment for cast text -> json';
