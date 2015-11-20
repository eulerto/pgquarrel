#include "trigger.h"

/*
 * CREATE TRIGGER
 * DROP TRIGGER
 * ALTER TRIGGER
 * COMMENT ON TRIGGER
 */

PQLTrigger *
getTriggers(PGconn *c, int *n)
{
	PQLTrigger	*t;
	PGresult	*res;
	int			i;

	logNoise("trigger: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				 "SELECT t.tgname AS trgname, n.nspname AS nspname, c.relname AS relname, pg_get_triggerdef(t.oid, false) AS trgdef, obj_description(t.oid, 'pg_rewrite') AS description FROM pg_trigger t INNER JOIN pg_class c ON (t.tgrelid = c.oid) INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE NOT tgisinternal");

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
		t = (PQLTrigger *) malloc(*n * sizeof(PQLTrigger));
	else
		t = NULL;

	logDebug("number of triggers in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		t[i].trgname = strdup(PQgetvalue(res, i, PQfnumber(res, "tgname")));
		t[i].table.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].table.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		t[i].trgdef = strdup(PQgetvalue(res, i, PQfnumber(res, "trgdef")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
			t[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));
		logDebug("trigger %s on %s.%s",
				formatObjectIdentifier(t[i].trgname),
				formatObjectIdentifier(t[i].table.schemaname),
				formatObjectIdentifier(t[i].table.objectname));
	}

	PQclear(res);

	return t;
}

void
dumpCreateTrigger(FILE *output, PQLTrigger t)
{
	fprintf(output, "\n\n");
	fprintf(output, "%s;", t.trgdef);

	/* comment */
	if (options.comment && t.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TRIGGER %s ON %s.%s IS '%s';",
				formatObjectIdentifier(t.trgname),
				formatObjectIdentifier(t.table.schemaname),
				formatObjectIdentifier(t.table.objectname),
				t.comment);
	}
}

void
dumpDropTrigger(FILE *output, PQLTrigger t)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP TRIGGER %s ON %s.%s;", t.trgname, t.table.schemaname, t.table.objectname);
}

void
dumpAlterTrigger(FILE *output, PQLTrigger a, PQLTrigger b)
{
	fprintf(output, "\n\n");
	fprintf(output, "ALTER TRIGGER %s ON %s.%s RENAME TO %s;", a.trgname, b.table.schemaname, b.table.objectname, b.trgname);

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TRIGGER %s ON %s.%s IS '%s';",
					formatObjectIdentifier(b.trgname),
				formatObjectIdentifier(b.table.schemaname),
				formatObjectIdentifier(b.table.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TRIGGER %s ON %s.%s IS NULL;",
					formatObjectIdentifier(b.trgname),
					formatObjectIdentifier(b.table.schemaname),
					formatObjectIdentifier(b.table.objectname));
		}
	}
}
