/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * am.c
 *     Generate ACCESS METHOD commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE ACCESS METHOD
 * DROP ACCESS METHOD
 * COMMENT ON ACCESS METHOD
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "am.h"

PQLAccessMethod *
getAccessMethods(PGconn *c, int *n)
{
	PQLAccessMethod		*a;
	PGresult			*res;
	int					i;

	logNoise("am: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 90600)
	{
		logWarning("ignoring access method because server does not support it");
		return NULL;
	}

	res = PQexec(c, "SELECT a.oid, a.amname, a.amtype, a.amhandler AS handleroid, n.nspname AS handlernspname, p.proname AS handlername, obj_description(a.oid, 'pg_am') AS description FROM pg_am a INNER JOIN pg_proc p ON (a.amhandler = p.oid) INNER JON pg_namespace n ON (p.pronamespace = n.oid) ORDER BY a.amname");

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
		a = (PQLAccessMethod *) malloc(*n * sizeof(PQLAccessMethod));
	else
		a = NULL;

	logDebug("number of access methods in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		a[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		a[i].amname = strdup(PQgetvalue(res, i, PQfnumber(res, "amname")));
		a[i].amtype = PQgetvalue(res, i, PQfnumber(res, "amtype"))[0];
		a[i].handler.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "handleroid")),
								   NULL, 10);
		a[i].handler.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
										 "handlernspname")));
		a[i].handler.objectname = strdup(PQgetvalue(res, i, PQfnumber(res,
										 "handlername")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			a[i].comment = NULL;
		else
		{
			a[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));
			a[i].comment = escapeQuotes(a[i].comment);
		}

		logDebug("access method \"%s\"", a[i].amname);
	}

	PQclear(res);

	return a;
}

void
freeAccessMethods(PQLAccessMethod *a, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(a[i].amname);
			if (a[i].handler.schemaname)
				free(a[i].handler.schemaname);
			if (a[i].handler.objectname)
				free(a[i].handler.objectname);
			if (a[i].comment)
				free(a[i].comment);
		}

		free(a);
	}
}

void
dumpDropAccessMethod(FILE *output, PQLAccessMethod *a)
{
	char	*amname = formatObjectIdentifier(a->amname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP ACCESS METHOD %s;", amname);

	free(amname);
}

void
dumpCreateAccessMethod(FILE *output, PQLAccessMethod *a)
{
	char	*amname = formatObjectIdentifier(a->amname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE ACCESS METHOD %s", amname);
	switch (a->amtype)
	{
		case PGQ_AMTYPE_INDEX:
			fprintf(output, " TYPE INDEX");
			break;
		case PGQ_AMTYPE_TABLE:
			fprintf(output, " TYPE TABLE");
			break;
		default:	/* can't happen */
			logWarning("bogus type value in pg_am.amtype");
	}

	fprintf(output, " HANDLER %s.%s", a->handler.schemaname, a->handler.objectname);

	fprintf(output, ";");

	/* comment */
	if (options.comment && a->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON ACCESS METHOD %s IS '%s';", amname,
				a->comment);
	}

	free(amname);
}

void
dumpAlterAccessMethod(FILE *output, PQLAccessMethod *a, PQLAccessMethod *b)
{
	/*
	 * XXX there is no ALTER ACCESS METHOD command. In this case, we need to
	 * XXX DROP and CREATE the access method.
	 */
	if (a->amname != b->amname ||
			a->amtype != b->amtype ||
			strcmp(a->handler.schemaname, b->handler.schemaname) != 0 ||
			strcmp(a->handler.objectname, b->handler.objectname) != 0)
	{
		dumpDropAccessMethod(output, a);
		dumpCreateAccessMethod(output, b);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON ACCESS METHOD %s IS '%s';",
					b->amname,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON ACCESS METHOD %s IS NULL;",
					b->amname);
		}
	}
}
