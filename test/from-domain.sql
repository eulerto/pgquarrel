CREATE DOMAIN from_domain_1 AS numeric(10, 2) NOT NULL;

--
-- test ALTER DOMAIN
--
CREATE DOMAIN same_domain_1 AS integer DEFAULT 1234;

CREATE DOMAIN same_domain_2 AS integer NOT NULL DEFAULT 1234;

CREATE DOMAIN same_domain_3 AS text CONSTRAINT same_domain_3_cnt NOT NULL;

COMMENT ON DOMAIN same_domain_1 IS 'this is comment for same_domain_1';

GRANT ALL PRIVILEGES ON DOMAIN same_domain_1 TO same_role_2;
