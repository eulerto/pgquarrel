/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * function.c
 *     Generate FUNCTION commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE FUNCTION
 * DROP FUNCTION
 * ALTER FUNCTION
 * COMMENT ON FUNCTION
 *
 * TODO
 *
 * ALTER FUNCTION ... RENAME TO
 * ALTER FUNCTION ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "function.h"


PQLFunction *
getFunctions(PGconn *c, int *n)
{
	PQLFunction	*f;
	PGresult	*res;
	int			i;

	logNoise("function: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90600)	/* parallel is new in 9.6 ('u'nsafe is the default) */
	{
		res = PQexec(c,
					 "SELECT p.oid, nspname, proname, proretset, prosrc, pg_get_function_arguments(p.oid) as funcargs, pg_get_function_identity_arguments(p.oid) as funciargs, pg_get_function_result(p.oid) as funcresult, proiswindow, provolatile, proisstrict, prosecdef, proleakproof, array_to_string(proconfig, ',') AS proconfig, proparallel, procost, prorows, (SELECT lanname FROM pg_language WHERE oid = prolang) AS lanname, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(proowner) AS proowner, proacl FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, proname, pg_get_function_identity_arguments(p.oid)");
	}
	else if (PQserverVersion(c) >= 90200)	/* proleakproof is new in 9.2 */
	{
		res = PQexec(c,
					 "SELECT p.oid, nspname, proname, proretset, prosrc, pg_get_function_arguments(p.oid) as funcargs, pg_get_function_identity_arguments(p.oid) as funciargs, pg_get_function_result(p.oid) as funcresult, proiswindow, provolatile, proisstrict, prosecdef, proleakproof, array_to_string(proconfig, ',') AS proconfig, 'u' AS proparallel, procost, prorows, (SELECT lanname FROM pg_language WHERE oid = prolang) AS lanname, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(proowner) AS proowner, proacl FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, proname, pg_get_function_identity_arguments(p.oid)");
	}
	else if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		res = PQexec(c,
					 "SELECT p.oid, nspname, proname, proretset, prosrc, pg_get_function_arguments(p.oid) as funcargs, pg_get_function_identity_arguments(p.oid) as funciargs, pg_get_function_result(p.oid) as funcresult, proiswindow, provolatile, proisstrict, prosecdef, false AS proleakproof, array_to_string(proconfig, ',') AS proconfig, 'u' AS proparallel, procost, prorows, (SELECT lanname FROM pg_language WHERE oid = prolang) AS lanname, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(proowner) AS proowner, proacl FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, proname, pg_get_function_identity_arguments(p.oid)");
	}
	else
	{
		res = PQexec(c,
					 "SELECT p.oid, nspname, proname, proretset, prosrc, pg_get_function_arguments(p.oid) as funcargs, pg_get_function_identity_arguments(p.oid) as funciargs, pg_get_function_result(p.oid) as funcresult, proiswindow, provolatile, proisstrict, prosecdef, false AS proleakproof, array_to_string(proconfig, ',') AS proconfig, 'u' AS proparallel, procost, prorows, (SELECT lanname FROM pg_language WHERE oid = prolang) AS lanname, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(proowner) AS proowner, proacl FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY nspname, proname, pg_get_function_identity_arguments(p.oid)");
	}

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
		f[i].iarguments = strdup(PQgetvalue(res, i, PQfnumber(res,
											"funciargs")));	/* don't print defaults */
		f[i].body = strdup(PQgetvalue(res, i, PQfnumber(res, "prosrc")));
		f[i].returntype = strdup(PQgetvalue(res, i, PQfnumber(res, "funcresult")));
		f[i].language = strdup(PQgetvalue(res, i, PQfnumber(res, "lanname")));
		f[i].funcvolatile = PQgetvalue(res, i, PQfnumber(res, "provolatile"))[0];
		f[i].iswindow = (PQgetvalue(res, i, PQfnumber(res, "proiswindow"))[0] == 't');
		f[i].isstrict = (PQgetvalue(res, i, PQfnumber(res, "proisstrict"))[0] == 't');
		f[i].secdefiner = (PQgetvalue(res, i, PQfnumber(res, "prosecdef"))[0] == 't');
		f[i].leakproof = (PQgetvalue(res, i, PQfnumber(res, "proleakproof"))[0] == 't');
		f[i].parallel = PQgetvalue(res, i, PQfnumber(res, "proparallel"))[0];
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

		/*
		 * Security labels are not assigned here (see getFunctionSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeFunctions.
		 */
		f[i].nseclabels = 0;
		f[i].seclabels = NULL;

		logDebug("function \"%s\".\"%s\"(%s)", f[i].obj.schemaname, f[i].obj.objectname,
				 f[i].arguments);
	}

	PQclear(res);

	return f;
}

int
compareFunctions(PQLFunction *a, PQLFunction *b)
{
	int		c;

	c = strcmp(a->obj.schemaname, b->obj.schemaname);

	/* compare relation names iif schema names are equal */
	if (c == 0)
	{
		c = strcmp(a->obj.objectname, b->obj.objectname);

		/* compare arguments iif schema-qualified names are equal */
		if (c == 0)
			c = strcmp(a->iarguments, b->iarguments);
	}

	return c;
}

void
getFunctionSecurityLabels(PGconn *c, PQLFunction *f)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	snprintf(query, 200,
			 "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_proc' AND s.objoid = %u ORDER BY provider",
			 f->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	f->nseclabels = PQntuples(res);
	if (f->nseclabels > 0)
		f->seclabels = (PQLSecLabel *) malloc(f->nseclabels * sizeof(PQLSecLabel));
	else
		f->seclabels = NULL;

	logDebug("number of security labels in function \"%s\".\"%s\"(%s): %d",
			 f->obj.schemaname, f->obj.objectname, f->arguments, f->nseclabels);

	for (i = 0; i < f->nseclabels; i++)
	{
		f->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		f->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

void
freeFunctions(PQLFunction *f, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(f[i].obj.schemaname);
			free(f[i].obj.objectname);
			free(f[i].arguments);
			free(f[i].iarguments);
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

			/* security labels */
			for (j = 0; j < f[i].nseclabels; j++)
			{
				free(f[i].seclabels[j].provider);
				free(f[i].seclabels[j].label);
			}

			if (f[i].seclabels)
				free(f[i].seclabels);
		}

		free(f);
	}
}

void
dumpDropFunction(FILE *output, PQLFunction *f)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*funcname = formatObjectIdentifier(f->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP FUNCTION %s.%s(%s);",
			schema,
			funcname,
			f->iarguments);

	free(schema);
	free(funcname);
}
void
dumpCreateFunction(FILE *output, PQLFunction *f, bool orreplace)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*funcname = formatObjectIdentifier(f->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE %sFUNCTION %s.%s(%s) RETURNS %s",
			orreplace ? "OR REPLACE " : "",
			schema, funcname, f->arguments, f->returntype);
	fprintf(output, "\n    LANGUAGE %s", f->language);
	if (f->iswindow)
		fprintf(output, " WINDOW");
	if (f->funcvolatile == 'i')
		fprintf(output, " IMMUTABLE");
	else if (f->funcvolatile == 's')
		fprintf(output, " STABLE");
	else if (f->funcvolatile == 'v')
		fprintf(output, " VOLATILE");
	else	/* can't happen */
		logError("unrecognized volatile value for function %s.%s(%s)",
				 schema, funcname, f->arguments);

	if (f->isstrict)
		fprintf(output, " STRICT");
	if (f->secdefiner)
		fprintf(output, " SECURITY DEFINER");
	if (f->leakproof)
		fprintf(output, " LEAKPROOF");

	/*
	 * Since PARALLEL UNSAFE (u) is the default, consider 'u' in old versions
	 * (<= 9.5) because default is not printed and makes it easy to compare
	 * different versions.
	 */
	if (f->parallel == 's')
		fprintf(output, " PARALLEL SAFE");
	else if (f->parallel == 'r')
		fprintf(output, " PARALLEL RESTRICTED");
	else if (f->parallel == 'u')	/* PARALLEL UNSAFE is the default */
		;

	if ((strcmp(f->language, "internal") == 0) || (strcmp(f->language, "c") == 0))
	{
		if (strcmp(f->cost, "1") != 0)
			fprintf(output, " COST %s", f->cost);
	}
	else
	{
		if (strcmp(f->cost, "100") != 0)
			fprintf(output, " COST %s", f->cost);
	}

	if (strcmp(f->rows, "0") != 0)
		fprintf(output, " ROWS %s", f->rows);

	if (f->configparams != NULL)
	{
		stringList		*sl;
		stringListCell	*cell;

		sl = buildStringList(f->configparams);
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

	fprintf(output, "\nAS $$%s$$;", f->body);

	/* comment */
	if (options.comment && f->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON FUNCTION %s.%s(%s) IS '%s';",
				schema,
				funcname,
				f->iarguments,
				f->comment);
	}

	/* security labels */
	if (options.securitylabels && f->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < f->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS '%s';",
					f->seclabels[i].provider,
					schema,
					funcname,
					f->iarguments,
					f->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FUNCTION %s.%s(%s) OWNER TO %s;",
				schema,
				funcname,
				f->iarguments,
				f->owner);
	}

	/* privileges */
	/* XXX second f->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_FUNCTION, &f->obj, &f->obj, NULL, f->acl,
						   f->iarguments, NULL);

	free(schema);
	free(funcname);
}

void
dumpAlterFunction(FILE *output, PQLFunction *a, PQLFunction *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*funcname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*funcname2 = formatObjectIdentifier(b->obj.objectname);

	bool	printalter = true;

	if (a->funcvolatile != b->funcvolatile)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		if (b->funcvolatile == 'i')
			fprintf(output, " IMMUTABLE");
		else if (b->funcvolatile == 's')
			fprintf(output, " STABLE");
		else if (b->funcvolatile == 'v')
			fprintf(output, " VOLATILE");
		else
			logError("volatile cannot be '%s'", b->funcvolatile);
	}

	if (a->isstrict != b->isstrict)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		if (b->isstrict)
			fprintf(output, " STRICT");
		else
			fprintf(output, "CALLED ON NULL INPUT");
	}

	if (a->secdefiner != b->secdefiner)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		if (b->secdefiner)
			fprintf(output, " SECURITY DEFINER");
		else
			fprintf(output, " SECURITY INVOKER");
	}

	/* FIXME leakproof new in 9.2 */
	if (a->leakproof != b->leakproof)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		if (b->leakproof)
			fprintf(output, " LEAKPROOF");
		else
			fprintf(output, " NOT LEAKPROOF");
	}

	if (a->parallel != b->parallel)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		if (b->parallel == 's')
			fprintf(output, " PARALLEL SAFE");
		else if (b->parallel == 'r')
			fprintf(output, " PARALLEL RESTRICTED");
		else if (b->parallel == 'u')
			fprintf(output, " PARALLEL UNSAFE");
		else
			logError("parallel cannot be '%c'", b->parallel);
	}

	if (strcmp(a->cost, b->cost) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		fprintf(output, " COST %s", b->cost);
	}

	if (strcmp(a->rows, b->rows) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		fprintf(output, " ROWS %s", b->rows);
	}

	/* configuration parameters */
	if (a->configparams != NULL && b->configparams == NULL)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		fprintf(output, " RESET ALL");
	}
	else if (a->configparams == NULL && b->configparams != NULL)
	{
		stringList	*sl;

		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		sl = buildStringList(b->configparams);
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
	else if (a->configparams != NULL && b->configparams != NULL &&
			 strcmp(a->configparams, b->configparams) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s)",
					schema2, funcname2, b->iarguments);
		}
		printalter = false;

		/*
		 * Reset options that are only presented in the first set.
		 */
		rlist = setOperationOptions(a->configparams, b->configparams, PGQ_SETDIFFERENCE,
									false, true);
		if (rlist)
		{
			stringListCell	*cell;

			for (cell = rlist->head; cell; cell = cell->next)
				fprintf(output, " RESET %s", cell->value);

			freeStringList(rlist);
		}

		/*
		 * Include intersection between parameter sets. However, exclude
		 * options that don't change.
		 */
		ilist = setOperationOptions(a->configparams, b->configparams, PGQ_INTERSECT,
									true, true);
		if (ilist)
		{
			stringListCell	*cell;

			for (cell = ilist->head; cell; cell = cell->next)
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

			freeStringList(ilist);
		}

		/*
		 * Set options that are only presented in the second set.
		 */
		slist = setOperationOptions(b->configparams, a->configparams, PGQ_SETDIFFERENCE,
									true, true);
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

	if (strcmp(a->body, b->body) != 0)
		dumpCreateFunction(output, b, true);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON FUNCTION %s.%s(%s) IS '%s';",
					schema2, funcname2, b->iarguments, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON FUNCTION %s.%s(%s) IS NULL;",
					schema2, funcname2, b->iarguments);
		}
	}

	/* security labels */
	if (options.securitylabels)
	{
		if (a->seclabels == NULL && b->seclabels != NULL)
		{
			int	i;

			for (i = 0; i < b->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS '%s';",
						b->seclabels[i].provider,
						schema2, funcname2, b->iarguments, b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS NULL;",
						a->seclabels[i].provider,
						schema1, funcname1, a->iarguments);
			}
		}
		else if (a->seclabels != NULL && b->seclabels != NULL)
		{
			int	i, j;

			i = j = 0;
			while (i < a->nseclabels || j < b->nseclabels)
			{
				if (i == a->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS '%s';",
							b->seclabels[j].provider,
							schema2, funcname2, b->iarguments, b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS NULL;",
							a->seclabels[i].provider,
							schema1, funcname1, a->iarguments);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS '%s';",
								b->seclabels[j].provider,
								schema2, funcname2, b->iarguments, b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS NULL;",
							a->seclabels[i].provider,
							schema1, funcname1, a->iarguments);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON FUNCTION %s.%s(%s) IS '%s';",
							b->seclabels[j].provider,
							schema2, funcname2, b->iarguments, b->seclabels[j].label);
					j++;
				}
			}
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FUNCTION %s.%s(%s) OWNER TO %s;",
					schema2, funcname2, b->iarguments, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_FUNCTION, &a->obj, &b->obj, a->acl, b->acl,
							   a->iarguments, NULL);
	}

	free(schema1);
	free(funcname1);
	free(schema2);
	free(funcname2);
}
