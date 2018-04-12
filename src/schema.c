/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * schema.c
 *     Generate SCHEMA commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE SCHEMA
 * DROP SCHEMA
 * ALTER SCHEMA
 * COMMENT ON SCHEMA
 *
 * TODO
 *
 * ALTER SCHEMA ... RENAME TO
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "schema.h"


PQLSchema *
getSchemas(PGconn *c, int *n)
{
	PQLSchema	*s;
	PGresult	*res;
	int			i;

	logNoise("schema: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		res = PQexec(c,
					 "SELECT n.oid, nspname, obj_description(n.oid, 'pg_namespace') AS description, pg_get_userbyid(nspowner) AS nspowner, nspacl FROM pg_namespace n WHERE nspname !~ '^pg_' AND nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE n.oid = d.objid AND d.deptype = 'e') ORDER BY nspname");
	}
	else
	{
		res = PQexec(c,
					 "SELECT n.oid, nspname, obj_description(n.oid, 'pg_namespace') AS description, pg_get_userbyid(nspowner) AS nspowner, nspacl FROM pg_namespace n WHERE nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname");
	}

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
		s = (PQLSchema *) malloc(*n * sizeof(PQLSchema));
	else
		s = NULL;

	logDebug("number of schemas in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		s[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		s[i].schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			s[i].comment = NULL;
		else
			s[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		s[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "nspowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "nspacl")))
			s[i].acl = NULL;
		else
			s[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "nspacl")));

		/*
		 * Security labels are not assigned here (see getSchemaSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeSchemas.
		 */
		s[i].nseclabels = 0;
		s[i].seclabels = NULL;

		logDebug("schema \"%s\"", s[i].schemaname);
	}

	PQclear(res);

	return s;
}

void
getSchemaSecurityLabels(PGconn *c, PQLSchema *s)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	snprintf(query, 200,
			 "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_namespace' AND s.objoid = %u ORDER BY provider",
			 s->oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	s->nseclabels = PQntuples(res);
	if (s->nseclabels > 0)
		s->seclabels = (PQLSecLabel *) malloc(s->nseclabels * sizeof(PQLSecLabel));
	else
		s->seclabels = NULL;

	logDebug("number of security labels in schema \"%s\": %d", s->schemaname,
			 s->nseclabels);

	for (i = 0; i < s->nseclabels; i++)
	{
		s->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		s->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

void
freeSchemas(PQLSchema *s, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(s[i].schemaname);
			if (s[i].comment)
				free(s[i].comment);
			if (s[i].acl)
				free(s[i].acl);
			free(s[i].owner);

			/* security labels */
			for (j = 0; j < s[i].nseclabels; j++)
			{
				free(s[i].seclabels[j].provider);
				free(s[i].seclabels[j].label);
			}

			if (s[i].seclabels)
				free(s[i].seclabels);
		}

		free(s);
	}
}

void
dumpDropSchema(FILE *output, PQLSchema *s)
{
	char	*schemaname = formatObjectIdentifier(s->schemaname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP SCHEMA %s;", schemaname);

	free(schemaname);
}

void
dumpCreateSchema(FILE *output, PQLSchema *s)
{
	char	*schemaname = formatObjectIdentifier(s->schemaname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE SCHEMA %s;", schemaname);

	/* comment */
	if (options.comment && s->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON SCHEMA %s IS '%s';", schemaname, s->comment);
	}

	/* security labels */
	if (options.securitylabels && s->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < s->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS '%s';",
					s->seclabels[i].provider,
					schemaname,
					s->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SCHEMA %s OWNER TO %s;", schemaname, s->owner);
	}

	/* privileges */
	/* XXX second s->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
	{
		PQLObject tmp;

		/*
		 * don't use schemaname because objects without qualification use
		 * objectname as name.
		 * */
		tmp.schemaname = NULL;
		tmp.objectname = s->schemaname;

		dumpGrantAndRevoke(output, PGQ_SCHEMA, &tmp, &tmp, NULL, s->acl, NULL, NULL);
	}

	free(schemaname);
}

void
dumpAlterSchema(FILE *output, PQLSchema *a, PQLSchema *b)
{
	char	*schemaname1 = formatObjectIdentifier(a->schemaname);
	char	*schemaname2 = formatObjectIdentifier(b->schemaname);

	if (strcmp(a->schemaname, b->schemaname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SCHEMA %s RENAME TO %s;", schemaname1, schemaname2);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SCHEMA %s IS '%s';", schemaname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SCHEMA %s IS NULL;", schemaname2);
		}
	}

	/* security labels */
	if (options.securitylabels)
	{
		if (a->seclabels == NULL && b->seclabels != NULL)
		{
			int	i;

			for (i = 0; i < b->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS '%s';",
						b->seclabels[i].provider,
						schemaname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS NULL;",
						a->seclabels[i].provider,
						schemaname1);
			}
		}
		else if (a->seclabels != NULL && b->seclabels != NULL)
		{
			int	i, j;

			i = j = 0;
			while (i < a->nseclabels || j < b->nseclabels)
			{
				if (i == a->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS '%s';",
							b->seclabels[j].provider,
							schemaname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS NULL;",
							a->seclabels[i].provider,
							schemaname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS '%s';",
								b->seclabels[j].provider,
								schemaname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS NULL;",
							a->seclabels[i].provider,
							schemaname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SCHEMA %s IS '%s';",
							b->seclabels[j].provider,
							schemaname2,
							b->seclabels[j].label);
					j++;
				}
			}
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SCHEMA %s OWNER TO %s;", schemaname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		PQLObject tmpa, tmpb;

		/*
		 * don't use schemaname because objects without qualification use
		 * objectname as name.
		 * */
		tmpa.schemaname = NULL;
		tmpa.objectname = a->schemaname;
		tmpb.schemaname = NULL;
		tmpb.objectname = b->schemaname;

		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_SCHEMA, &tmpa, &tmpb, a->acl, b->acl, NULL,
							   NULL);
	}

	free(schemaname1);
	free(schemaname2);
}
