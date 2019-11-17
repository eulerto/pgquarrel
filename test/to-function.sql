--
-- Function
--
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

GRANT EXECUTE ON FUNCTION to_function_1(integer, integer) TO same_role_1, same_role_2, same_role_3;
GRANT ALL PRIVILEGES ON FUNCTION same_function_1(integer, integer) TO same_role_1;
GRANT EXECUTE ON FUNCTION same_function_1(integer, integer) TO same_role_3;

--
-- Procedure
--
CREATE PROCEDURE to_proc_1(args1 integer, args2 integer, INOUT args3 integer) AS
$$
BEGIN
	args3 := args1 * args2 + args1;
END
$$ LANGUAGE plpgsql;

CREATE PROCEDURE to_proc_2(args1 integer, args2 integer, INOUT args3 integer) AS
$$
BEGIN
	args3 := args1 * args1 + args2;
END
$$ LANGUAGE plpgsql;

CREATE PROCEDURE same_proc_1(args1 integer, args2 integer DEFAULT 10, INOUT args3 integer DEFAULT 0) AS
$$
BEGIN
	args3 := args1 + args1 + args2;
END
$$ LANGUAGE plpgsql;

CREATE PROCEDURE same_proc_2(args1 integer, args2 integer, INOUT args3 integer) AS
$$
BEGIN
	args3 := args1 - args2;
END
$$ LANGUAGE plpgsql;

CREATE PROCEDURE same_proc_3(args1 integer, args2 integer, INOUT args3 integer) AS
$$
BEGIN
	args3 := args1 * args2 * args1;
END
$$ LANGUAGE plpgsql;

COMMENT ON PROCEDURE same_proc_2(integer, integer, integer) IS 'this is comment for same_proc_2';

GRANT EXECUTE ON PROCEDURE to_proc_1(integer, integer, integer) TO same_role_1, same_role_2, same_role_3;
GRANT ALL PRIVILEGES ON PROCEDURE same_proc_1(integer, integer, integer) TO same_role_1;
GRANT EXECUTE ON PROCEDURE same_proc_1(integer, integer, integer) TO same_role_3;
