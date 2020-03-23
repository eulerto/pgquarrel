/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * domain.c
 *     Generate DOMAIN commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE DOMAIN
 * DROP DOMAIN
 * ALTER DOMAIN ... { SET DEFAULT expression | DROP DEFAULT }
 * ALTER DOMAIN ... { SET | DROP } NOT NULL
 * COMMENT ON DOMAIN
 *
 * TODO
 *
 * ALTER DOMAIN ... ADD CONSTRAINT
 * ALTER DOMAIN ... DROP CONSTRAINT
 * ALTER DOMAIN ... RENAME CONSTRAINT
 * ALTER DOMAIN ... VALIDATE CONSTRAINT
 * ALTER DOMAIN ... RENAME TO
 * ALTER DOMAIN ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "domain.h"


PQLDomain *
getDomains(PGconn *c, int *n)
{
	PQLDomain		*d;
	char			*query;
	PGresult		*res;
	int				i;

	logNoise("domain: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90200)		/* support for privileges on data types */
	{
		query = psprintf("SELECT t.oid, n.nspname, t.typname, format_type(t.typbasetype, t.typtypmod) as domaindef, t.typnotnull, CASE WHEN t.typcollation <> u.typcollation THEN '\"' || p.nspname || '\".\"' || l.collname || '\"' ELSE NULL END AS typcollation, pg_get_expr(t.typdefaultbin, 'pg_type'::regclass) AS typdefault, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, t.typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) LEFT JOIN pg_type u ON (t.typbasetype = u.oid) LEFT JOIN pg_collation l ON (t.typcollation = l.oid) LEFT JOIN pg_namespace p ON (l.collnamespace = p.oid) WHERE t.typtype = 'd' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		/* typcollation is new in 9.1 */
		query = psprintf("SELECT t.oid, n.nspname, t.typname, format_type(t.typbasetype, t.typtypmod) as domaindef, t.typnotnull, CASE WHEN t.typcollation <> u.typcollation THEN '\"' || p.nspname || '\".\"' || l.collname || '\"' ELSE NULL END AS typcollation, pg_get_expr(t.typdefaultbin, 'pg_type'::regclass) AS typdefault, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) LEFT JOIN pg_type u ON (t.typbasetype = u.oid) LEFT JOIN pg_collation l ON (t.typcollation = l.oid) LEFT JOIN pg_namespace p ON (l.collnamespace = p.oid) WHERE t.typtype = 'd' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname", include_schema_str, exclude_schema_str);
	}
	else
	{
		query = psprintf("SELECT t.oid, n.nspname, t.typname, format_type(t.typbasetype, t.typtypmod) as domaindef, t.typnotnull, NULL AS typcollation, pg_get_expr(t.typdefaultbin, 'pg_type'::regclass) AS typdefault, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'd' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s ORDER BY n.nspname, t.typname", include_schema_str, exclude_schema_str);
	}

	res = PQexec(c, query);

	pfree(query);

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
		d = (PQLDomain *) malloc(*n * sizeof(PQLDomain));
	else
		d = NULL;

	logDebug("number of domains in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		d[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		d[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		d[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "typname")));
		d[i].domaindef = strdup(PQgetvalue(res, i, PQfnumber(res, "domaindef")));
		d[i].notnull = (PQgetvalue(res, i, PQfnumber(res, "typnotnull"))[0] == 't');

		if (PQgetisnull(res, i, PQfnumber(res, "typcollation")))
			d[i].collation = NULL;
		else
			d[i].collation = strdup(PQgetvalue(res, i, PQfnumber(res, "typcollation")));

		if (PQgetisnull(res, i, PQfnumber(res, "typdefault")))
			d[i].ddefault = NULL;
		else
			d[i].ddefault = strdup(PQgetvalue(res, i, PQfnumber(res, "typdefault")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			d[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			d[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (d[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		d[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "typowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "typacl")))
			d[i].acl = NULL;
		else
			d[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "typacl")));

		/*
		 * These values are not assigned here (see getDomainConstraints), but
		 * default values are essential to avoid having trouble in
		 * freeDomains.
		 */
		d[i].ncheck = 0;
		d[i].check = NULL;

		/*
		 * Security labels are not assigned here (see getDomainSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeDomains.
		 */
		d[i].nseclabels = 0;
		d[i].seclabels = NULL;

		logDebug("domain \"%s\".\"%s\"", d[i].obj.schemaname, d[i].obj.objectname);
	}

	PQclear(res);

	return d;
}

void
getDomainConstraints(PGconn *c, PQLDomain *d)
{
	char		*query;
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) >= 90100)
	{
		query = psprintf("SELECT conname, pg_get_constraintdef(oid) AS condef, convalidated FROM pg_constraint WHERE contypid = %u ORDER BY conname",
						  d->obj.oid);
	}
	else
	{
		query = psprintf("SELECT conname, pg_get_constraintdef(oid) AS condef, true AS convalidated FROM pg_constraint WHERE contypid = %u ORDER BY conname",
						  d->obj.oid);
	}

	res = PQexec(c, query);

	pfree(query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	d->ncheck = PQntuples(res);
	if (d->ncheck > 0)
		d->check = (PQLConstraint *) malloc(d->ncheck * sizeof(PQLConstraint));
	else
		d->check = NULL;

	logDebug("number of check constraints in domain \"%s\".\"%s\": %d",
			 d->obj.schemaname, d->obj.objectname, d->ncheck);

	for (i = 0; i < d->ncheck; i++)
	{
		d->check[i].conname = strdup(PQgetvalue(res, i, PQfnumber(res, "conname")));
		d->check[i].condef = strdup(PQgetvalue(res, i, PQfnumber(res, "condef")));
		d->check[i].convalidated = (PQgetvalue(res, i, PQfnumber(res,
											   "convalidated"))[0] == 't');
	}

	PQclear(res);
}

void
getDomainSecurityLabels(PGconn *c, PQLDomain *d)
{
	char		*query;
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	/*
	 * Don't bother to check the kind of type because can't be duplicated oids
	 * in the same catalog.
	 */
	query = psprintf("SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_type' AND s.objoid = %u ORDER BY provider",
			 d->obj.oid);

	res = PQexec(c, query);

	pfree(query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	d->nseclabels = PQntuples(res);
	if (d->nseclabels > 0)
		d->seclabels = (PQLSecLabel *) malloc(d->nseclabels * sizeof(PQLSecLabel));
	else
		d->seclabels = NULL;

	logDebug("number of security labels in domain \"%s\".\"%s\": %d",
			 d->obj.schemaname, d->obj.objectname, d->nseclabels);

	for (i = 0; i < d->nseclabels; i++)
	{
		char	*withoutescape;

		d->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		withoutescape = PQgetvalue(res, i, PQfnumber(res, "label"));
		d->seclabels[i].label = PQescapeLiteral(c, withoutescape,
												strlen(withoutescape));
		if (d->seclabels[i].label == NULL)
		{
			logError("escaping label failed: %s", PQerrorMessage(c));
			PQclear(res);
			PQfinish(c);
			/* XXX leak another connection? */
			exit(EXIT_FAILURE);
		}
	}

	PQclear(res);
}

void
freeDomains(PQLDomain *d, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(d[i].obj.schemaname);
			free(d[i].obj.objectname);
			free(d[i].domaindef);
			if (d[i].collation)
				free(d[i].collation);
			if (d[i].ddefault)
				free(d[i].ddefault);
			if (d[i].comment)
				PQfreemem(d[i].comment);
			free(d[i].owner);
			if (d[i].acl)
				free(d[i].acl);

			for (j = 0 ; j < d[i].ncheck; j++)
			{
				free(d[i].check[j].conname);
				free(d[i].check[j].condef);
			}

			if (d[i].check)
				free(d[i].check);

			/* security labels */
			for (j = 0; j < d[i].nseclabels; j++)
			{
				free(d[i].seclabels[j].provider);
				PQfreemem(d[i].seclabels[j].label);
			}

			if (d[i].seclabels)
				free(d[i].seclabels);
		}

		free(d);
	}
}

void
dumpCreateDomain(FILE *output, PQLDomain *d)
{
	int		i;
	char	*schema = formatObjectIdentifier(d->obj.schemaname);
	char	*domname = formatObjectIdentifier(d->obj.objectname);


	fprintf(output, "\n\n");
	fprintf(output, "CREATE DOMAIN %s.%s AS %s",
			schema,
			domname,
			d->domaindef);

	if (d->collation != NULL)
	{
		/* collation namespace is already included */
		fprintf(output, " COLLATE %s", d->collation);
	}

	if (d->notnull)
		fprintf(output, " NOT NULL");

	if (d->ddefault != NULL)
		fprintf(output, " DEFAULT %s", d->ddefault);

	/* CHECK constraint */
	/* XXX consider dump it separately? */
	for (i = 0; i < d->ncheck; i++)
		fprintf(output, "\n\tCONSTRAINT %s %s", d->check[i].conname,
				d->check[i].condef);

	fprintf(output, ";");

	/* comment */
	if (options.comment && d->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON DOMAIN %s.%s IS %s;",
				schema,
				domname,
				d->comment);
	}

	/* security labels */
	if (options.securitylabels && d->nseclabels > 0)
	{
		for (i = 0; i < d->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS %s;",
					d->seclabels[i].provider,
					schema,
					domname,
					d->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(d->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER DOMAIN %s.%s OWNER TO %s;",
				schema,
				domname,
				owner);

		free(owner);
	}

	/* privileges */
	/* XXX second d->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_DOMAIN, &d->obj, &d->obj, NULL, d->acl, NULL,
						   NULL);

	free(schema);
	free(domname);
}

void
dumpDropDomain(FILE *output, PQLDomain *d)
{
	char	*schema = formatObjectIdentifier(d->obj.schemaname);
	char	*domname = formatObjectIdentifier(d->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP DOMAIN %s.%s;", schema, domname);

	free(schema);
	free(domname);
}

void
dumpAlterDomain(FILE *output, PQLDomain *a, PQLDomain *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*domname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*domname2 = formatObjectIdentifier(b->obj.objectname);

	if ((a->ddefault == NULL && b->ddefault != NULL) ||
			(a->ddefault != NULL && b->ddefault == NULL) ||
			(a->ddefault != NULL && b->ddefault != NULL &&
			 strcmp(a->ddefault, b->ddefault) != 0))
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER DOMAIN %s.%s", schema2, domname2);

		if (b->ddefault != NULL)
			fprintf(output, " SET DEFAULT %s", b->ddefault);
		else
			fprintf(output, " DROP DEFAULT");

		fprintf(output, ";");
	}

	if (a->notnull != b->notnull)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER DOMAIN %s.%s", schema2, domname2);

		if (b->notnull)
			fprintf(output, " SET NOT NULL");
		else
			fprintf(output, " DROP NOT NULL");

		fprintf(output, ";");
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON DOMAIN %s.%s IS %s;",
					schema2,
					domname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON DOMAIN %s.%s IS NULL;",
					schema2,
					domname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS %s;",
						b->seclabels[i].provider,
						schema2,
						domname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						domname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS %s;",
							b->seclabels[j].provider,
							schema2,
							domname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							domname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS %s;",
								b->seclabels[j].provider,
								schema2,
								domname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							domname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON DOMAIN %s.%s IS %s;",
							b->seclabels[j].provider,
							schema2,
							domname2,
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
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER DOMAIN %s.%s OWNER TO %s;",
					schema2,
					domname2,
					owner);

			free(owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_DOMAIN, &a->obj, &b->obj, a->acl, b->acl, NULL,
							   NULL);
	}

	free(schema1);
	free(domname1);
	free(schema2);
	free(domname2);
}
