/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * transform.c
 *     Generate TRANSFORM commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE TRANSFORM
 * DROP TRANSFORM
 * COMMENT ON TRANSFORM
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "transform.h"


PQLTransform *
getTransforms(PGconn *c, int *n)
{
	PQLTransform	*t;
	PGresult		*res;
	int				i;

	logNoise("transform: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 90500)
	{
		logWarning("ignoring transforms because server does not support it");
		return NULL;
	}

	res = PQexec(c, "SELECT t.oid, n.nspname AS typschema, y.typname AS typname, (SELECT lanname FROM pg_language WHERE oid = t.trflang) AS lanname, p.oid AS fromsqloid, x.nspname AS fromsqlschema, p.proname AS fromsqlname, pg_get_function_arguments(t.trffromsql) AS fromsqlargs, q.oid AS tosqloid, z.nspname AS tosqlschema, q.proname AS tosqlname, pg_get_function_args(t.trftosql) AS tosqlargs, obj_description(t.oid, 'pg_transform') AS description FROM pg_transform t INNER JOIN pg_type y ON (t.trftype = y.oid) INNER JOIN pg_namespace n ON (n.oid = y.typnamespace) LEFT JOIN pg_proc p ON (t.trffromsql = p.oid) LEFT JOIN pg_namespace x ON (x.oid = p.pronamespace) LEFT JOIN pg_proc q ON (t.trftosql = q.oid) LEFT JOIN pg_namespace z ON (z.oid = q.pronamespace) ORDER BY typschema, typname, lanname");

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
		t = (PQLTransform *) malloc(*n * sizeof(PQLTransform));
	else
		t = NULL;

	logDebug("number of transforms in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		t[i].trftype.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].trftype.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "typschema")));
		t[i].trftype.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "typname")));
		t[i].languagename = strdup(PQgetvalue(res, i, PQfnumber(res, "lanname")));

		if (PQgetisnull(res, i, PQfnumber(res, "fromsqlname")))
		{
			t[i].fromsql.schemaname = NULL;
			t[i].fromsql.objectname = NULL;
			t[i].fromsqlargs = NULL;
		}
		else
		{
			t[i].fromsql.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "fromsqloid")), NULL, 10);
			t[i].fromsql.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "fromsqlschema")));
			t[i].fromsql.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "fromsqlname")));
			t[i].fromsqlargs = strdup(PQgetvalue(res, i, PQfnumber(res, "fromsqlargs")));
		}

		if (PQgetisnull(res, i, PQfnumber(res, "tosqlname")))
		{
			t[i].tosql.schemaname = NULL;
			t[i].tosql.objectname = NULL;
			t[i].tosqlargs = NULL;
		}
		else
		{
			t[i].tosql.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "tosqloid")), NULL, 10);
			t[i].tosql.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "tosqlschema")));
			t[i].tosql.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "tosqlname")));
			t[i].tosqlargs = strdup(PQgetvalue(res, i, PQfnumber(res, "tosqlargs")));
		}

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			t[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (t[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		logDebug("transform for type \"%s\".\"%s\" language \"%s\"", t[i].trftype.schemaname, t[i].trftype.objectname, t[i].languagename);
	}

	PQclear(res);

	return t;
}

void
freeTransforms(PQLTransform *t, int n)
{
	if (n > 0)
	{
		int i;

		for (i = 0; i < n; i++)
		{
			free(t[i].trftype.schemaname);
			free(t[i].trftype.objectname);
			free(t[i].languagename);
			if (t[i].fromsql.schemaname)
				free(t[i].fromsql.schemaname);
			if (t[i].fromsql.objectname)
				free(t[i].fromsql.objectname);
			if (t[i].fromsqlargs)
				free(t[i].fromsqlargs);
			if (t[i].tosql.schemaname)
				free(t[i].tosql.schemaname);
			if (t[i].tosql.objectname)
				free(t[i].tosql.objectname);
			if (t[i].tosqlargs)
				free(t[i].tosqlargs);
			if (t[i].comment)
				PQfreemem(t[i].comment);
		}

		free(t);
	}
}

void
dumpDropTransform(FILE *output, PQLTransform *t)
{
	char	*typeschema = formatObjectIdentifier(t->trftype.schemaname);
	char	*typename = formatObjectIdentifier(t->trftype.objectname);
	char	*langname = formatObjectIdentifier(t->languagename);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TRANSFORM FOR %s.%s LANGUAGE %s;", typeschema, typename, langname);

	free(typeschema);
	free(typename);
	free(langname);
}

void
dumpCreateTransform(FILE *output, PQLTransform *t)
{
	char	*typeschema = formatObjectIdentifier(t->trftype.schemaname);
	char	*typename = formatObjectIdentifier(t->trftype.objectname);
	char	*langname = formatObjectIdentifier(t->languagename);
	char	*fromsqlschema = formatObjectIdentifier(t->fromsql.schemaname);
	char	*fromsqlname = formatObjectIdentifier(t->fromsql.objectname);
	char	*tosqlschema = formatObjectIdentifier(t->tosql.schemaname);
	char	*tosqlname = formatObjectIdentifier(t->tosql.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TRANSFORM FOR %s.%s LANGUAGE %s (", typeschema, typename, langname);
	if (t->fromsql.objectname != NULL)
		fprintf(output, "FROM SQL WITH FUNCTION %s.%s", fromsqlschema, fromsqlname);
	if (t->tosql.objectname != NULL)
		fprintf(output, "TO SQL WITH FUNCTION %s.%s", tosqlschema, tosqlname);
	fprintf(output, ");");

	/* comment */
	if (options.comment && t->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TRANSFORM FOR %s.%s LANGUAGE %s IS %s;", typeschema, typename, langname, t->comment);
	}

	free(typeschema);
	free(typename);
	free(langname);
	free(fromsqlschema);
	free(fromsqlname);
	free(tosqlschema);
	free(tosqlname);
}

void
dumpAlterTransform(FILE *output, PQLTransform *a, PQLTransform *b)
{
}
