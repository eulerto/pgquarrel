CREATE LANGUAGE plperl;
CREATE LANGUAGE plperlu;

--GRANT USAGE ON LANGUAGE plperl TO same_role_1;
--GRANT USAGE ON LANGUAGE plperl TO same_role_3;
--REVOKE ALL PRIVILEGES ON LANGUAGE plperl FROM PUBLIC;
--GRANT USAGE ON LANGUAGE plpgsql TO same_role_1;

COMMENT ON LANGUAGE plperlu IS 'this is a comment for untrusted PL/Perl';
