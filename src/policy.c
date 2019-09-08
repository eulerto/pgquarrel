/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * policy.c
 *     Generate POLICY commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE POLICY
 * DROP POLICY
 * ALTER POLICY
 * COMMENT ON POLICY
 *
 * TODO
 *
 * ALTER POLICY ... RENAME TO
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "policy.h"


PQLPolicy *
getPolicies(PGconn *c, int *n)
{
	PQLPolicy	*p;
	PGresult	*res;
	int			i;

	logNoise("policy: server version: %d", PQserverVersion(c));

	res = PQexec(c, "SELECT p.oid, p.polname, p.polrelid, n.nspname AS polnamespace, c.relname AS poltabname, p.polcmd, p.polpermissive, CASE WHEN p.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(ARRAY(SELECT pg_catalog.quote_ident(rolname) from pg_catalog.pg_roles WHERE oid = ANY(p.polroles)), ', ') END AS polroles, pg_catalog.pg_get_expr(p.polqual, p.polrelid) AS polqual, pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid) AS polwithcheck FROM pg_policy p INNER JOIN pg_class c ON (p.polrelid = c.oid) INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) ORDER BY p.polname");

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
		p = (PQLPolicy *) malloc(*n * sizeof(PQLPolicy));
	else
		p = NULL;

	logDebug("number of policies in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		p[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		p[i].polname = strdup(PQgetvalue(res, i, PQfnumber(res, "polname")));
		p[i].table.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "polrelid")), NULL, 10);
		p[i].table.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "polnspname")));
		p[i].table.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "poltabname")));
		p[i].cmd = PQgetvalue(res, i, PQfnumber(res, "polcmd"))[0];
		p[i].permissive = (PQgetvalue(res, i, PQfnumber(res, "polpermissive"))[0] == 't');
		if (PQgetisnull(res, i, PQfnumber(res, "polroles")))
			p[i].roles = NULL;
		else
			p[i].roles = strdup(PQgetvalue(res, i, PQfnumber(res, "polroles")));
		if (PQgetisnull(res, i, PQfnumber(res, "polqual")))
			p[i].qual = NULL;
		else
			p[i].qual = strdup(PQgetvalue(res, i, PQfnumber(res, "polqual")));
		if (PQgetisnull(res, i, PQfnumber(res, "polwithcheck")))
			p[i].withcheck = NULL;
		else
			p[i].withcheck = strdup(PQgetvalue(res, i, PQfnumber(res, "polwithcheck")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			p[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			p[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (p[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		logDebug("policy \"%s\" on \"%s\".\"%s\"", p[i].polname, p[i].table.schemaname,
				 p[i].table.objectname);
	}

	PQclear(res);

	return p;
}

void
freePolicies(PQLPolicy *p, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(p[i].polname);
			free(p[i].table.schemaname);
			free(p[i].table.objectname);
			free(p[i].roles);
			free(p[i].qual);
			free(p[i].withcheck);
			if (p[i].comment)
				PQfreemem(p[i].comment);
		}

		free(p);
	}
}

void
dumpCreatePolicy(FILE *output, PQLPolicy *p)
{
	char	*polname = formatObjectIdentifier(p->polname);
	char	*schema = formatObjectIdentifier(p->table.schemaname);
	char	*tabname = formatObjectIdentifier(p->table.objectname);
	char	*cmd = NULL;

	if (p->cmd == '*')
		cmd = strdup("");
	else if (p->cmd == 'r')
		cmd = strdup(" FOR SELECT");
	else if (p->cmd == 'a')
		cmd = strdup(" FOR INSERT");
	else if (p->cmd == 'w')
		cmd = strdup(" FOR UPDATE");
	else if (p->cmd == 'd')
		cmd = strdup(" FOR DELETE");
	else
	{
		logError("bogus value in pg_policy.polcmd (%c) in policy %s", p->cmd, p->polname);
		return;
	}

	fprintf(output, "\n\n");
	fprintf(output, "CREATE POLICY %s;", p->polname);
	fprintf(output, " ON %s.%s%s%s", schema, tabname, (!p->permissive ? " AS RESTRICTIVE" : ""), cmd);

	if (p->roles != NULL)
		fprintf(output, " TO %s", p->roles);

	if (p->qual != NULL)
		fprintf(output, " USING (%s)", p->qual);

	if (p->withcheck != NULL)
		fprintf(output, " WITH CHECK (%s)", p->withcheck);

	fprintf(output, ";");

	/* comment */
	if (options.comment && p->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON POLICY %s ON %s.%s IS %s;", polname, schema,
				tabname, p->comment);
	}

	free(polname);
	free(schema);
	free(tabname);
	free(cmd);
}

void
dumpDropPolicy(FILE *output, PQLPolicy *t)
{
	char	*polname = formatObjectIdentifier(t->polname);
	char	*schema = formatObjectIdentifier(t->table.schemaname);
	char	*tabname = formatObjectIdentifier(t->table.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP POLICY %s ON %s.%s;", polname, schema, tabname);

	free(polname);
	free(schema);
	free(tabname);
}

void
dumpAlterPolicy(FILE *output, PQLPolicy *a, PQLPolicy *b)
{
	char	*polname1 = formatObjectIdentifier(a->polname);
	char	*polname2 = formatObjectIdentifier(b->polname);
	char	*schema2 = formatObjectIdentifier(b->table.schemaname);
	char	*tabname2 = formatObjectIdentifier(b->table.objectname);
	bool	first = true;

	if ((a->roles == NULL && b->roles != NULL) ||
			(a->roles != NULL && b->roles == NULL) ||
			(strcmp(a->roles, b->roles) != 0))
	{
		if (first)
			fprintf(output, "\n\nALTER POLICY %s ON %s.%s", polname2, schema2, tabname2);
		else
			first = false;

		fprintf(output, " TO %s", b->roles);
	}

	if ((a->qual == NULL && b->qual != NULL) ||
			(a->qual != NULL && b->qual == NULL) ||
			(strcmp(a->qual, b->qual) != 0))
	{
		if (first)
			fprintf(output, "\n\nALTER POLICY %s ON %s.%s", polname2, schema2, tabname2);
		else
			first = false;

		fprintf(output, " USING (%s)", b->qual);
	}

	if ((a->withcheck == NULL && b->withcheck != NULL) ||
			(a->withcheck != NULL && b->withcheck == NULL) ||
			(strcmp(a->withcheck, b->withcheck) != 0))
	{
		if (first)
			fprintf(output, "\n\nALTER POLICY %s ON %s.%s", polname2, schema2, tabname2);
		else
			first = false;

		fprintf(output, " WITH CHECK (%s)", b->withcheck);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON POLICY %s ON %s.%s IS %s;", polname2, schema2,
					tabname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON POLICY %s ON %s.%s IS NULL;", polname2, schema2,
					tabname2);
		}
	}

	free(polname1);
	free(polname2);
	free(schema2);
	free(tabname2);
}
