--
-- Function
--
CREATE FUNCTION from_function_1(args1 integer, args2 integer DEFAULT 10) RETURNS integer AS
$$
BEGIN
	RETURN args1 / args2;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION same_function_1(args1 integer, args2 integer) RETURNS integer AS
$$
BEGIN
	RETURN args1 + args2;
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
	RETURN args1 * args2;
END
$$ LANGUAGE plpgsql
SET work_mem TO '50MB'
SET enable_seqscan TO false;

COMMENT ON FUNCTION same_function_1(integer, integer) IS 'this is comment for same_function_1';

GRANT ALL PRIVILEGES ON FUNCTION same_function_1(integer, integer) TO same_role_2;

--
-- Procedure
--
CREATE PROCEDURE from_proc_1(args1 integer, args2 integer, INOUT args3 integer) AS
$$
BEGIN
	args3 := args1 / args2;
END
$$ LANGUAGE plpgsql;

CREATE PROCEDURE same_proc_1(args1 integer, args2 integer, INOUT args3 integer) AS
$$
BEGIN
	args3 := args1 + args2;
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
	args3 := args1 * args2;
END
$$ LANGUAGE plpgsql;

COMMENT ON PROCEDURE same_proc_1(integer, integer, integer) IS 'this is comment for same_proc_1';

GRANT ALL PRIVILEGES ON PROCEDURE same_proc_1(integer, integer, integer) TO same_role_2;
