CREATE SCHEMA from_schema_1;
CREATE SCHEMA same_schema_1;

GRANT ALL PRIVILEGES ON SCHEMA same_schema_1 TO same_role_1;
GRANT USAGE ON SCHEMA same_schema_1 TO same_role_2;

CREATE ROLE "from-quoted-role";
GRANT USAGE ON SCHEMA from_schema_1 TO "from-quoted-role";
