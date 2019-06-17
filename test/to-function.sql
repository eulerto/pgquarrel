CREATE FUNCTION to_function_1(args1 integer, args2 integer) RETURNS integer AS
$$
BEGIN
	RETURN args1 * args2 + args1;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION to_function_2(args1 integer, args2 integer) RETURNS integer AS
$$
BEGIN
	RETURN args1 * args1 + args2;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION same_function_1(args1 integer, args2 integer DEFAULT 10) RETURNS integer AS
$$
BEGIN
	RETURN args1 + args1 + args2;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION same_function_2(args1 integer, args2 integer) RETURNS integer AS
$$
BEGIN
	RETURN args1 - args2;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION same_function_3(args1 integer, args2 integer) RETURNS integer AS
$$
BEGIN
	RETURN args1 * args2 * args1;
END
$$ LANGUAGE plpgsql
SET search_path TO public
SET work_mem TO '100MB';

COMMENT ON FUNCTION same_function_2(integer, integer) IS 'this is comment for same_function_2';

COMMENT ON FUNCTION same_function_3(integer, integer) IS 'this is comment for same_function_3 with ''';


GRANT EXECUTE ON FUNCTION to_function_1(integer, integer) TO same_role_1, same_role_2, same_role_3;
GRANT ALL PRIVILEGES ON FUNCTION same_function_1(integer, integer) TO same_role_1;
GRANT EXECUTE ON FUNCTION same_function_1(integer, integer) TO same_role_3;
