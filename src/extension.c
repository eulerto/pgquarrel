/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * extension.c
 *     Generate EXTENSION commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE EXTENSION
 * DROP EXTENSION
 * ALTER EXTENSION
 * COMMENT ON EXTENSION
 *
 * TODO
 *
 * ALTER EXTENSION ... ADD
 * ALTER EXTENSION ... DROP
 * ALTER EXTENSION ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "extension.h"


PQLExtension *
getExtensions(PGconn *c, int *n)
{
	PQLExtension	*e;
	PGresult		*res;
	int				i;

	logNoise("extension: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring extensions because server does not support it");
		return NULL;
	}

	res = PQexec(c,
				 "SELECT e.oid, extname AS extensionname, nspname, extversion AS version, extrelocatable, obj_description(e.oid, 'pg_extension') AS description FROM pg_extension e LEFT JOIN pg_namespace n ON (e.extnamespace = n.oid) ORDER BY extname");

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
		e = (PQLExtension *) malloc(*n * sizeof(PQLExtension));
	else
		e = NULL;

	logDebug("number of extensions in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		e[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		e[i].extensionname = strdup(PQgetvalue(res, i, PQfnumber(res,
											   "extensionname")));
		e[i].schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		e[i].version = strdup(PQgetvalue(res, i, PQfnumber(res, "version")));
		e[i].relocatable = (PQgetvalue(res, i, PQfnumber(res,
									   "extrelocatable"))[0] == 't');
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			e[i].comment = NULL;
		else
			e[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("extension \"%s\"", e[i].extensionname);
	}

	PQclear(res);

	return e;
}

void
freeExtensions(PQLExtension *e, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(e[i].extensionname);
			free(e[i].schemaname);
			free(e[i].version);
			if (e[i].comment)
				free(e[i].comment);
		}

		free(e);
	}
}

void
dumpDropExtension(FILE *output, PQLExtension *e)
{
	char	*extname = formatObjectIdentifier(e->extensionname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP EXTENSION %s;", extname);

	free(extname);
}

void
dumpCreateExtension(FILE *output, PQLExtension *e)
{
	char	*extname = formatObjectIdentifier(e->extensionname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE EXTENSION %s", extname);

	if (e->relocatable)
		fprintf(output, " WITH SCHEMA %s", e->schemaname);

	fprintf(output, " VERSION '%s'", e->version);

	fprintf(output, ";");

	/* comment */
	if (options.comment && e->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON EXTENSION %s IS '%s';", extname, e->comment);
	}

	free(extname);
}

void
dumpAlterExtension(FILE *output, PQLExtension *a, PQLExtension *b)
{
	char	*extname2 = formatObjectIdentifier(b->extensionname);

	if (strcmp(a->version, b->version) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EXTENSION %s UPDATE TO %s;", extname2, b->version);
	}

	if (strcmp(a->schemaname, b->schemaname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EXTENSION %s SET SCHEMA %s;", extname2, b->schemaname);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EXTENSION %s IS '%s';", extname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EXTENSION %s IS NULL;", extname2);
		}
	}

	free(extname2);
}
