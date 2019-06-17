CREATE CAST (json AS character varying) WITHOUT FUNCTION;

CREATE CAST (json AS text) WITHOUT FUNCTION AS IMPLICIT;

CREATE CAST (float AS text) WITHOUT FUNCTION AS IMPLICIT;

COMMENT ON CAST (json AS text) IS 'this is comment for cast json -> text';

COMMENT ON CAST (float AS text) IS 'this is comment for cast float -> text';