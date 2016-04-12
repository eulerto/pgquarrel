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
				 "SELECT t.oid, t.tgname AS trgname, n.nspname AS nspname, c.relname AS relname, pg_get_triggerdef(t.oid, false) AS trgdef, obj_description(t.oid, 'pg_rewrite') AS description FROM pg_trigger t INNER JOIN pg_class c ON (t.tgrelid = c.oid) INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE NOT tgisinternal");

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
		t[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].trgname = strdup(PQgetvalue(res, i, PQfnumber(res, "tgname")));
		t[i].table.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].table.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		t[i].trgdef = strdup(PQgetvalue(res, i, PQfnumber(res, "trgdef")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
			t[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("trigger \"%s\" on \"%s\".\"%s\"", t[i].trgname, t[i].table.schemaname, t[i].table.objectname);
	}

	PQclear(res);

	return t;
}

void
freeTriggers(PQLTrigger *t, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(t[i].trgname);
			free(t[i].table.schemaname);
			free(t[i].table.objectname);
			free(t[i].trgdef);
			if (t[i].comment)
				free(t[i].comment);
		}

		free(t);
	}
}

void
dumpCreateTrigger(FILE *output, PQLTrigger t)
{
	char	*trgname = formatObjectIdentifier(t.trgname);
	char	*schema = formatObjectIdentifier(t.table.schemaname);
	char	*tabname = formatObjectIdentifier(t.table.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "%s;", t.trgdef);

	/* comment */
	if (options.comment && t.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TRIGGER %s ON %s.%s IS '%s';", trgname, schema, tabname, t.comment);
	}

	free(trgname);
	free(schema);
	free(tabname);
}

void
dumpDropTrigger(FILE *output, PQLTrigger t)
{
	char	*trgname = formatObjectIdentifier(t.trgname);
	char	*schema = formatObjectIdentifier(t.table.schemaname);
	char	*tabname = formatObjectIdentifier(t.table.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TRIGGER %s ON %s.%s;", trgname, schema, tabname);

	free(trgname);
	free(schema);
	free(tabname);
}

void
dumpAlterTrigger(FILE *output, PQLTrigger a, PQLTrigger b)
{
	char	*trgname1 = formatObjectIdentifier(a.trgname);
	char	*trgname2 = formatObjectIdentifier(b.trgname);
	char	*schema2 = formatObjectIdentifier(b.table.schemaname);
	char	*tabname2 = formatObjectIdentifier(b.table.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TRIGGER %s ON %s.%s RENAME TO %s;", trgname1, schema2, tabname2, trgname2);

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TRIGGER %s ON %s.%s IS '%s';", trgname2, schema2, tabname2, b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TRIGGER %s ON %s.%s IS NULL;", trgname2, schema2, tabname2);
		}
	}

	free(trgname1);
	free(trgname2);
	free(schema2);
	free(tabname2);
}
