#include "extension.h"

/*
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
 */

PQLExtension *
getExtensions(PGconn *c, int *n)
{
	PQLExtension	*e;
	PGresult		*res;
	int				i;

	logNoise("extension: server version: %d", PQserverVersion(c));

	res = PQexec(c,
			"SELECT extname AS extensionname, nspname, extversion AS version, extrelocatable, description FROM pg_extension e LEFT JOIN pg_namespace n ON (e.extnamespace = n.oid) LEFT JOIN (pg_description d INNER JOIN pg_class x ON (x.oid = d.classoid AND x.relname = 'pg_extension')) ON (d.objoid = e.oid) ORDER BY extname");

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
		e[i].extensionname = strdup(PQgetvalue(res, i, PQfnumber(res, "extensionname")));
		e[i].schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		e[i].version = strdup(PQgetvalue(res, i, PQfnumber(res, "version")));
		e[i].relocatable = (PQgetvalue(res, i, PQfnumber(res, "extrelocatable"))[0] == 't');
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			e[i].comment = NULL;
		else
			e[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("extension %s", formatObjectIdentifier(e[i].extensionname));
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
dumpDropExtension(FILE *output, PQLExtension e)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP EXTENSION %s;",
			formatObjectIdentifier(e.extensionname));
}

void
dumpCreateExtension(FILE *output, PQLExtension e)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE EXTENSION %s",
			formatObjectIdentifier(e.extensionname));

	if (e.relocatable)
		fprintf(output, " WITH SCHEMA %s", e.schemaname);

	fprintf(output, " VERSION '%s'", e.version);

	fprintf(output, ";");

	/* comment */
	if (options.comment && e.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON EXTENSION %s IS '%s';",
				formatObjectIdentifier(e.extensionname), e.comment);
	}
}

void
dumpAlterExtension(FILE *output, PQLExtension a, PQLExtension b)
{
	if (strcmp(a.version, b.version) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EXTENSION %s UPDATE TO %s;",
				formatObjectIdentifier(b.extensionname),
				b.version);
	}

	if (strcmp(a.schemaname, b.schemaname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EXTENSION %s SET SCHEMA %s;",
				formatObjectIdentifier(b.extensionname),
				b.schemaname);
	}

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EXTENSION %s IS '%s';",
					formatObjectIdentifier(b.extensionname), b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EXTENSION %s IS NULL;",
					formatObjectIdentifier(b.extensionname));
		}
	}
}
