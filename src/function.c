#include "function.h"

/*
 * CREATE FUNCTION
 * DROP FUNCTION
 * ALTER FUNCTION
 * COMMENT ON FUNCTION
 *
 * TODO
 *
 * ALTER FUNCTION ... RENAME TO
 * ALTER FUNCTION ... SET SCHEMA
 */

PQLFunction *
getFunctions(PGconn *c, int *n)
{
	PQLFunction	*f;
	PGresult	*res;
	int			i;

	logNoise("function: server version: %d", PQserverVersion(c));

	/* proleakproof is new in 9.2 */
	if (PQserverVersion(c) >= 90200)
		res = PQexec(c,
					 "SELECT p.oid, nspname, proname, proretset, prosrc, pg_get_function_arguments(p.oid) as funcargs, pg_get_function_result(p.oid) as funcresult, proiswindow, provolatile, proisstrict, prosecdef, proleakproof, array_to_string(proconfig, ',') AS proconfig, procost, prorows, (SELECT lanname FROM pg_language WHERE oid = prolang) AS lanname, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(proowner) AS proowner, proacl FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, proname, pg_get_function_arguments(p.oid)");
	else
		res = PQexec(c,
					 "SELECT p.oid, nspname, proname, proretset, prosrc, pg_get_function_arguments(p.oid) as funcargs, pg_get_function_result(p.oid) as funcresult, proiswindow, provolatile, proisstrict, prosecdef, false AS proleakproof, array_to_string(proconfig, ',') AS proconfig, procost, prorows, (SELECT lanname FROM pg_language WHERE oid = prolang) AS lanname, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(proowner) AS proowner, proacl FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, proname, pg_get_function_arguments(p.oid)");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	*n = PQntuples(res);
	if (*n > 0)
		f = (PQLFunction *) malloc(*n * sizeof(PQLFunction));
	else
		f = NULL;

	logDebug("number of functions in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		f[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		f[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		f[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "proname")));
		f[i].arguments = strdup(PQgetvalue(res, i, PQfnumber(res, "funcargs")));
		f[i].body = strdup(PQgetvalue(res, i, PQfnumber(res, "prosrc")));
		f[i].returntype = strdup(PQgetvalue(res, i, PQfnumber(res, "funcresult")));
		f[i].language = strdup(PQgetvalue(res, i, PQfnumber(res, "lanname")));
		f[i].funcvolatile = PQgetvalue(res, i, PQfnumber(res, "provolatile"))[0];
		f[i].iswindow = (PQgetvalue(res, i, PQfnumber(res, "proiswindow"))[0] == 't');
		f[i].isstrict = (PQgetvalue(res, i, PQfnumber(res, "proisstrict"))[0] == 't');
		f[i].secdefiner = (PQgetvalue(res, i, PQfnumber(res, "prosecdef"))[0] == 't');
		f[i].leakproof = (PQgetvalue(res, i, PQfnumber(res, "proleakproof"))[0] == 't');
		f[i].cost = strdup(PQgetvalue(res, i, PQfnumber(res, "procost")));
		f[i].rows = strdup(PQgetvalue(res, i, PQfnumber(res, "prorows")));
		if (PQgetisnull(res, i, PQfnumber(res, "proconfig")))
			f[i].configparams = NULL;
		else
			f[i].configparams = strdup(PQgetvalue(res, i, PQfnumber(res, "proconfig")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			f[i].comment = NULL;
		else
			f[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		f[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "proowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "proacl")))
			f[i].acl = NULL;
		else
			f[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "proacl")));

		logDebug("function %s.%s(%s)", formatObjectIdentifier(f[i].obj.schemaname),
				 formatObjectIdentifier(f[i].obj.objectname), f[i].arguments);
	}

	PQclear(res);

	return f;
}

int
compareFunctions(PQLFunction a, PQLFunction b)
{
	int		c;

	c = strcmp(a.obj.schemaname, b.obj.schemaname);

	/* compare relation names iif schema names are equal */
	if (c == 0)
	{
		c = strcmp(a.obj.objectname, b.obj.objectname);

		/* compare arguments iif schema-qualified names are equal */
		if (c == 0)
			c = strcmp(a.arguments, b.arguments);
	}

	return c;
}

void
freeFunctions(PQLFunction *f, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(f[i].obj.schemaname);
			free(f[i].obj.objectname);
			free(f[i].arguments);
			free(f[i].body);
			free(f[i].returntype);
			free(f[i].language);
			free(f[i].cost);
			free(f[i].rows);
			if (f[i].configparams)
				free(f[i].configparams);
			if (f[i].comment)
				free(f[i].comment);
			free(f[i].owner);
			if (f[i].acl)
				free(f[i].acl);
		}

		free(f);
	}
}

void
getFunctionAttributes(PGconn *c, PQLFunction *f)
{
}

void
dumpDropFunction(FILE *output, PQLFunction f)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP FUNCTION %s.%s(%s);",
			formatObjectIdentifier(f.obj.schemaname),
			formatObjectIdentifier(f.obj.objectname), f.arguments);
}
void
dumpCreateFunction(FILE *output, PQLFunction f, bool orreplace)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE %sFUNCTION %s.%s(%s) RETURNS %s",
			orreplace ? "OR REPLACE " : "", formatObjectIdentifier(f.obj.schemaname),
			formatObjectIdentifier(f.obj.objectname), f.arguments, f.returntype);
	fprintf(output, "\n    LANGUAGE %s", f.language);
	if (f.iswindow)
		fprintf(output, " WINDOW");
	if (f.funcvolatile == 'i')
		fprintf(output, " IMMUTABLE");
	else if (f.funcvolatile == 's')
		fprintf(output, " STABLE");
	else if (f.funcvolatile == 'v')
		fprintf(output, " VOLATILE");
	else	/* can't happen */
		logError("unrecognized volatile value for function %s.%s(%s)",
				 formatObjectIdentifier(f.obj.schemaname),
				 formatObjectIdentifier(f.obj.objectname), f.arguments);

	if (f.isstrict)
		fprintf(output, " STRICT");
	if (f.secdefiner)
		fprintf(output, " SECURITY DEFINER");
	if (f.leakproof)
		fprintf(output, " LEAKPROOF");

	if ((strcmp(f.language, "internal") == 0) || (strcmp(f.language, "c") == 0))
	{
		if (strcmp(f.cost, "1") != 0)
			fprintf(output, " COST %s", f.cost);
	}
	else
	{
		if (strcmp(f.cost, "100") != 0)
			fprintf(output, " COST %s", f.cost);
	}

	if (strcmp(f.rows, "0") != 0)
		fprintf(output, " ROWS %s", f.rows);

	if (f.configparams != NULL)
	{
		stringList		*sl;
		stringListCell	*cell;

		sl = buildRelOptions(f.configparams);
		for (cell = sl->head; cell; cell = cell->next)
		{
			char	*str;

			str = strchr(cell->value, '=');
			if (str == NULL)
				continue;
			*str++ = '\0';

			fprintf(output, " SET %s TO ", cell->value);

			if (strcasecmp(cell->value, "DateStyle") == 0 ||
					strcasecmp(cell->value, "search_path") == 0)
				fprintf(output, "%s", str);
			else
				fprintf(output, "'%s'", str);
		}

		freeStringList(sl);
	}

	fprintf(output, "\nAS $$%s$$;", f.body);

	/* comment */
	if (options.comment && f.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON FUNCTION %s.%s(%s) IS '%s';",
				formatObjectIdentifier(f.obj.schemaname),
				formatObjectIdentifier(f.obj.objectname),
				f.arguments,
				f.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FUNCTION %s.%s(%s) OWNER TO %s;",
				formatObjectIdentifier(f.obj.schemaname),
				formatObjectIdentifier(f.obj.objectname),
				f.arguments,
				f.owner);
	}

	/* privileges */
	/* XXX second f.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_FUNCTION, f.obj, f.obj, NULL, f.acl, f.arguments);
}

void
dumpAlterFunction(FILE *output, PQLFunction a, PQLFunction b)
{
	bool	printalter = true;

	if (a.funcvolatile != b.funcvolatile)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		if (b.funcvolatile == 'i')
			fprintf(output, " IMMUTABLE");
		else if (b.funcvolatile == 's')
			fprintf(output, " STABLE");
		else if (b.funcvolatile == 'v')
			fprintf(output, " VOLATILE");
		else
			logError("volatile cannot be '%s'", b.funcvolatile);
	}

	if (a.isstrict != b.isstrict)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		if (b.isstrict)
			fprintf(output, " STRICT");
		else
			fprintf(output, "CALLED ON NULL INPUT");
	}

	if (a.secdefiner != b.secdefiner)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		if (b.secdefiner)
			fprintf(output, " SECURITY DEFINER");
		else
			fprintf(output, " SECURITY INVOKER");
	}

	/* FIXME leakproof new in 9.2 */
	if (a.leakproof != b.leakproof)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		if (b.leakproof)
			fprintf(output, " LEAKPROOF");
		else
			fprintf(output, " NOT LEAKPROOF");
	}

	if (strcmp(a.cost, b.cost) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		fprintf(output, " COST %s", b.cost);
	}

	if (strcmp(a.rows, b.rows) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		fprintf(output, " ROWS %s", b.rows);
	}

	/* configuration parameters */
	if (a.configparams != NULL && b.configparams == NULL)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		fprintf(output, " RESET ALL");
	}
	else if (a.configparams == NULL && b.configparams != NULL)
	{
		stringList	*sl;

		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		sl = buildRelOptions(b.configparams);
		if (sl)
		{
			stringListCell	*cell;

			for (cell = sl->head; cell; cell = cell->next)
			{
				char	*str;

				str = strchr(cell->value, '=');
				if (str == NULL)
					continue;
				*str++ = '\0';

				fprintf(output, " SET %s TO ", cell->value);

				if (strcasecmp(cell->value, "DateStyle") == 0 ||
						strcasecmp(cell->value, "search_path") == 0)
					fprintf(output, "%s", str);
				else
					fprintf(output, "'%s'", str);
			}

			freeStringList(sl);
		}
	}
	else if (a.configparams != NULL && b.configparams != NULL &&
			  strcmp(a.configparams, b.configparams) != 0)
	{
		stringList	*rlist, *slist;

		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname), b.arguments);
		}
		printalter = false;

		rlist = diffRelOptions(a.configparams, b.configparams, PGQ_EXCEPT);
		if (rlist)
		{
			stringListCell	*cell;

			for (cell = rlist->head; cell; cell = cell->next)
				fprintf(output, " RESET %s", cell->value);

			freeStringList(rlist);
		}

		/*
		 * FIXME we used to use diffRelOptions with PGQ_INTERSECT kind but it
		 * is buggy. Instead, we use all options from b. It is not wrong, but
		 * it would be nice to remove unnecessary options (e.g. same
		 * option/value).
		 */
		slist = buildRelOptions(b.configparams);
		if (slist)
		{
			stringListCell	*cell;

			for (cell = slist->head; cell; cell = cell->next)
			{
				char	*str;

				str = strchr(cell->value, '=');
				if (str == NULL)
					continue;
				*str++ = '\0';

				fprintf(output, " SET %s TO ", cell->value);

				if (strcasecmp(cell->value, "DateStyle") == 0 ||
						strcasecmp(cell->value, "search_path") == 0)
					fprintf(output, "%s", str);
				else
					fprintf(output, "'%s'", str);
			}

			freeStringList(slist);
		}
	}

	if (!printalter)
		fprintf(output, ";");

	if (strcmp(a.body, b.body) != 0)
		dumpCreateFunction(output, b, true);

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON FUNCTION %s.%s(%s) IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.arguments,
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON FUNCTION %s.%s(%s) IS NULL;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.arguments);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a.owner, b.owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s) OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.arguments,
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_FUNCTION, a.obj, b.obj, a.acl, b.acl, a.arguments);
	}
}
