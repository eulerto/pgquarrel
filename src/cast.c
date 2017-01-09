/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * cast.c
 *     Generate CAST commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE CAST
 * DROP CAST
 * COMMENT ON CAST
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2017, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "cast.h"


/* there are similar code here in common.c */
int
compareCasts(PQLCast *a, PQLCast *b)
{
	int		c;

	c = strcmp(a->source, b->source);

	/* compare target types iif source type names are equal */
	if (c == 0)
		c = strcmp(a->target, b->target);

	return c;
}

PQLCast *
getCasts(PGconn *c, int *n)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PQLCast		*d;
	PGresult	*res;
	int			i;
	int			r;

	logNoise("cast: server version: %d", PQserverVersion(c));

	do
	{
		query = (char *) malloc(nquery * sizeof(char));

		if (PQserverVersion(c) >= 90100)	/* extension support */
		{
			r = snprintf(query, nquery,
					 "SELECT c.oid, format_type(c.castsource, t.typtypmod) as source, format_type(c.casttarget, u.typtypmod) as target, castmethod, quote_ident(n.nspname) || '.' || quote_ident(f.proname) || '(' || pg_get_function_arguments(f.oid) || ')' as funcname, castcontext, obj_description(c.oid, 'pg_cast') AS description FROM pg_cast c LEFT JOIN pg_type t ON (c.castsource = t.oid) LEFT JOIN pg_type u ON (c.casttarget = u.oid) LEFT JOIN pg_proc f ON (c.castfunc = f.oid) LEFT JOIN pg_namespace n ON (f.pronamespace = n.oid) WHERE c.oid >= %u AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY source, target",
					 PGQ_FIRST_USER_OID);
		}
		else
		{
			r = snprintf(query, nquery,
					 "SELECT c.oid, format_type(c.castsource, t.typtypmod) as source, format_type(c.casttarget, u.typtypmod) as target, castmethod, quote_ident(n.nspname) || '.' || quote_ident(f.proname) || '(' || pg_get_function_arguments(f.oid) || ')' as funcname, castcontext, obj_description(c.oid, 'pg_cast') AS description FROM pg_cast c LEFT JOIN pg_type t ON (c.castsource = t.oid) LEFT JOIN pg_type u ON (c.casttarget = u.oid) LEFT JOIN pg_proc f ON (c.castfunc = f.oid) LEFT JOIN pg_namespace n ON (f.pronamespace = n.oid) WHERE c.oid >= %u ORDER BY source, target",
					 PGQ_FIRST_USER_OID);
		}

		if (r < nquery)
			break;

		logNoise("query size: required (%u) ; initial (%u)", r, nquery);
		nquery = r + 1;	/* make enough room for query */
		free(query);
	}
	while (true);

	res = PQexec(c, query);

	free(query);

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
		d = (PQLCast *) malloc(*n * sizeof(PQLCast));
	else
		d = NULL;

	logDebug("number of casts in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		d[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		d[i].source = strdup(PQgetvalue(res, i, PQfnumber(res, "source")));
		d[i].target = strdup(PQgetvalue(res, i, PQfnumber(res, "target")));
		d[i].method = PQgetvalue(res, i, PQfnumber(res, "castmethod"))[0];
		d[i].funcname = strdup(PQgetvalue(res, i, PQfnumber(res, "funcname")));
		d[i].context = PQgetvalue(res, i, PQfnumber(res, "castcontext"))[0];

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			d[i].comment = NULL;
		else
			d[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("cast \"%s\" as \"%s\" ; method: %c ; context: %c", d[i].source,
				 d[i].target, d[i].method, d[i].context);
	}

	PQclear(res);

	return d;
}

void
freeCasts(PQLCast *c, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(c[i].source);
			free(c[i].target);
			free(c[i].funcname);
			if (c[i].comment)
				free(c[i].comment);
		}

		free(c);
	}
}

void
dumpCreateCast(FILE *output, PQLCast *c)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE CAST (%s AS %s)", c->source, c->target);

	switch (c->method)
	{
		case PGQ_CAST_METHOD_BINARY:
			fprintf(output, " WITHOUT FUNCTION");
			break;
		case PGQ_CAST_METHOD_FUNCTION:
			if (c->funcname)
				fprintf(output, " WITH FUNCTION %s", c->funcname);
			else
				logWarning("bogus value in pg_cast.castfunc or pg_cast.castmethod");
			break;
		case PGQ_CAST_METHOD_INOUT:
			fprintf(output, " WITH INOUT");
			break;
		default:
			logWarning("bogus value in pg_cast.castmethod");
			break;
	}

	if (c->context == 'a')
		fprintf(output, " AS ASSIGNMENT");
	else if (c->context == 'i')
		fprintf(output, " AS IMPLICIT");
	else if (c->context == 'e')
		;
	else
		logWarning("bogus value in pg_cast.castcontext");

	fprintf(output, ";");

	/* comment */
	if (options.comment && c->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON CAST (%s AS %s) IS '%s';",
				c->source,
				c->target,
				c->comment);
	}
}

void
dumpDropCast(FILE *output, PQLCast *c)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP CAST (%s AS %s);", c->source, c->target);
}

void
dumpAlterCast(FILE *output, PQLCast *a, PQLCast *b)
{
	/*
	 * XXX there is no ALTER CAST command. In this case, we need to DROP and
	 * XXX CREATE the cast.
	 */
	if (a->method != b->method ||
			a->context != b->context ||
			(a->funcname == NULL && b->funcname != NULL) ||
			(a->funcname != NULL && b->funcname == NULL) ||
			(a->funcname != NULL && b->funcname != NULL &&
			 strcmp(a->funcname, b->funcname) != 0))
	{
		dumpDropCast(output, a);
		dumpCreateCast(output, b);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CAST (%s AS %s) IS '%s';",
					b->source,
					b->target,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CAST (%s AS %s) IS NULL;",
					b->source,
					b->target);
		}
	}
}
