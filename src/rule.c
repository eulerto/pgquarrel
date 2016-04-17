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
				 "SELECT r.oid, n.nspname AS schemaname, c.relname AS tablename, r.rulename, pg_get_ruledef(r.oid) AS definition, obj_description(r.oid, 'pg_rewrite') AS description FROM pg_rewrite r INNER JOIN pg_class c ON (c.oid = r.ev_class) INNER JOIN pg_namespace n ON (n.oid = c.relnamespace) WHERE r.rulename <> '_RETURN'::name AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY n.nspname, c.relname, r.rulename");

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
		r[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		r[i].table.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
									   "schemaname")));
		r[i].table.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "tablename")));
		r[i].rulename = strdup(PQgetvalue(res, i, PQfnumber(res, "rulename")));
		r[i].ruledef = strdup(PQgetvalue(res, i, PQfnumber(res, "definition")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			r[i].comment = NULL;
		else
			r[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("rule \"%s\" on \"%s\".\"%s\"", r[i].rulename, r[i].table.schemaname, r[i].table.objectname);
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
dumpDropRule(FILE *output, PQLRule *r)
{
	char	*schema = formatObjectIdentifier(r->table.schemaname);
	char	*objname = formatObjectIdentifier(r->table.objectname);
	char	*rulename = formatObjectIdentifier(r->rulename);

	fprintf(output, "\n\n");
	fprintf(output, "DROP RULE %s ON %s.%s;", rulename, schema, objname);

	free(schema);
	free(objname);
	free(rulename);
}

void
dumpCreateRule(FILE *output, PQLRule *r)
{
	char	*schema = formatObjectIdentifier(r->table.schemaname);
	char	*objname = formatObjectIdentifier(r->table.objectname);
	char	*rulename = formatObjectIdentifier(r->rulename);

	fprintf(output, "\n\n");
	fprintf(output, "%s", r->ruledef);

	/* comment */
	if (options.comment && r->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON RULE %s ON %s.%s IS '%s';", rulename, schema, objname, r->comment);
	}

	free(schema);
	free(objname);
	free(rulename);
}

void
dumpAlterRule(FILE *output, PQLRule *a, PQLRule *b)
{
	char	*schema = formatObjectIdentifier(b->table.schemaname);
	char	*objname = formatObjectIdentifier(b->table.objectname);
	char	*rulename1 = formatObjectIdentifier(a->rulename);
	char	*rulename2 = formatObjectIdentifier(b->rulename);

	if (strcmp(a->rulename, b->rulename) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER RULE %s ON %s.%s RENAME TO %s;", rulename1, schema, objname, rulename2);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON RULE %s ON %s.%s IS '%s';", rulename2, schema, objname, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON RULE %s ON %s.%s IS NULL;", rulename2, schema, objname);
		}
	}

	free(schema);
	free(objname);
	free(rulename1);
	free(rulename2);
}
