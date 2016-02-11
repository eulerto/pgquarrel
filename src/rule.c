#include "rule.h"

/*
 * CREATE RULE
 * DROP RULE
 * ALTER RULE RENAME TO
 * COMMENT ON RULE
 */

PQLRule *
getRules(PGconn *c, int *n)
{
	PQLRule		*r;
	PGresult	*res;
	int			i;

	logNoise("rule: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				 "SELECT n.nspname AS schemaname, c.relname AS tablename, r.rulename, pg_get_ruledef(r.oid) AS definition, obj_description(r.oid, 'pg_rewrite') AS description FROM pg_rewrite r INNER JOIN pg_class c ON (c.oid = r.ev_class) INNER JOIN pg_namespace n ON (n.oid = c.relnamespace) WHERE r.rulename <> '_RETURN'::name AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY n.nspname, c.relname, r.rulename");

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
		r = (PQLRule *) malloc(*n * sizeof(PQLRule));
	else
		r = NULL;

	logDebug("number of rules in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		r[i].table.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
									   "schemaname")));
		r[i].table.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "tablename")));
		r[i].rulename = strdup(PQgetvalue(res, i, PQfnumber(res, "rulename")));
		r[i].ruledef = strdup(PQgetvalue(res, i, PQfnumber(res, "definition")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			r[i].comment = NULL;
		else
			r[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("rule %s on %s.%s", formatObjectIdentifier(r[i].rulename),
				 formatObjectIdentifier(r[i].table.schemaname),
				 formatObjectIdentifier(r[i].table.objectname));
	}

	PQclear(res);

	return r;
}

void
freeRules(PQLRule *r, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(r[i].table.schemaname);
			free(r[i].table.objectname);
			free(r[i].rulename);
			free(r[i].ruledef);
			if (r[i].comment)
				free(r[i].comment);
		}

		free(r);
	}
}

void
dumpDropRule(FILE *output, PQLRule r)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP RULE %s ON %s.%s;",
			formatObjectIdentifier(r.rulename),
			formatObjectIdentifier(r.table.schemaname),
			formatObjectIdentifier(r.table.objectname));
}

void
dumpCreateRule(FILE *output, PQLRule r)
{
	fprintf(output, "\n\n");
	fprintf(output, "%s", r.ruledef);

	/* comment */
	if (options.comment && r.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON RULE %s ON %s.%s IS '%s';",
				formatObjectIdentifier(r.rulename),
				formatObjectIdentifier(r.table.schemaname),
				formatObjectIdentifier(r.table.objectname),
				r.comment);
	}
}

void
dumpAlterRule(FILE *output, PQLRule a, PQLRule b)
{
	if (strcmp(a.rulename, b.rulename) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER RULE %s ON %s.%s RENAME TO %s;",
				formatObjectIdentifier(a.rulename),
				formatObjectIdentifier(b.table.schemaname),
				formatObjectIdentifier(b.table.objectname),
				formatObjectIdentifier(b.rulename));
	}

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON RULE %s ON %s.%s IS '%s';",
					formatObjectIdentifier(b.rulename),
					formatObjectIdentifier(b.table.schemaname),
					formatObjectIdentifier(b.table.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON RULE %s ON %s.%s IS NULL;",
					formatObjectIdentifier(b.rulename),
					formatObjectIdentifier(b.table.schemaname),
					formatObjectIdentifier(b.table.objectname));
		}
	}
}
