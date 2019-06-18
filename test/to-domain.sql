CREATE DOMAIN to_domain_1 AS integer;

CREATE DOMAIN to_domain_2 AS numeric(10, 2) NOT NULL DEFAULT 123.45 CHECK(value > 0);

CREATE DOMAIN to_domain_3 AS varchar(250) COLLATE "en_US" CONSTRAINT to_domain_3_cnt NOT NULL;

--
-- test ALTER DOMAIN
--
CREATE DOMAIN same_domain_1 AS integer NOT NULL;

CREATE DOMAIN same_domain_2 AS integer DEFAULT 4321;

CREATE DOMAIN same_domain_3 AS text;

COMMENT ON DOMAIN same_domain_2 IS 'this is comment for same_domain_2';

GRANT USAGE ON DOMAIN to_domain_1 TO same_role_1, same_role_2, same_role_3;
GRANT ALL PRIVILEGES ON DOMAIN same_domain_1 TO same_role_1;
GRANT USAGE ON DOMAIN same_domain_1 TO same_role_3;
