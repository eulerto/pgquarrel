/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * view.c
 *     Generate VIEW commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE VIEW
 * DROP VIEW
 * ALTER VIEW
 * COMMENT ON VIEW
 *
 * TODO
 *
 * ALTER VIEW ... ALTER COLUMN ... { SET | DROP } DEFAULT
 * ALTER VIEW ... RENAME TO
 * ALTER VIEW ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "view.h"


PQLView *
getViews(PGconn *c, int *n)
{
	PQLView		*v;
	PGresult	*res;
	int			i;

	logNoise("view: server version: %d", PQserverVersion(c));

	/*
	 * FIXME exclude check_option from reloptions.
	 * check_option is new in 9.4
	 * array_remove() is new in 9.3
	 */
	if (PQserverVersion(c) >= 90300)
	{
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid) AS viewdef, array_to_string(array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded'), ', ') AS reloptions, CASE WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'v' AND nspname !~ '^pg_' AND nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, relname");
	}
	else if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid) AS viewdef, array_to_string(c.reloptions, ', ') AS reloptions, CASE WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'v' AND nspname !~ '^pg_' AND nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, relname");
	}
	else
	{
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid) AS viewdef, array_to_string(c.reloptions, ', ') AS reloptions, CASE WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'v' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");
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
		v = (PQLView *) malloc(*n * sizeof(PQLView));
	else
		v = NULL;

	logDebug("number of views in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		v[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		v[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		v[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		/* FIXME don't load it only iff view will be DROPped */
		v[i].viewdef = strdup(PQgetvalue(res, i, PQfnumber(res, "viewdef")));
		if (PQgetisnull(res, i, PQfnumber(res, "reloptions")))
			v[i].reloptions = NULL;
		else
			v[i].reloptions = strdup(PQgetvalue(res, i, PQfnumber(res, "reloptions")));
		if (PQgetisnull(res, i, PQfnumber(res, "checkoption")))
			v[i].checkoption = NULL;
		else
			v[i].checkoption = strdup(PQgetvalue(res, i, PQfnumber(res, "checkoption")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			v[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			v[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (v[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		v[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "relowner")));

		/*
		 * Security labels are not assigned here (see getViewSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeViews.
		 */
		v[i].nseclabels = 0;
		v[i].seclabels = NULL;

		logDebug("view \"%s\".\"%s\"", v[i].obj.schemaname, v[i].obj.objectname);
	}

	PQclear(res);

	return v;
}

void
getViewSecurityLabels(PGconn *c, PQLView *v)
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
			 "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_class' AND s.objoid = %u ORDER BY provider",
			 v->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	v->nseclabels = PQntuples(res);
	if (v->nseclabels > 0)
		v->seclabels = (PQLSecLabel *) malloc(v->nseclabels * sizeof(PQLSecLabel));
	else
		v->seclabels = NULL;

	logDebug("number of security labels in view \"%s\".\"%s\": %d",
			 v->obj.schemaname, v->obj.objectname, v->nseclabels);

	for (i = 0; i < v->nseclabels; i++)
	{
		char	*withoutescape;

		v->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		withoutescape = PQgetvalue(res, i, PQfnumber(res, "label"));
		v->seclabels[i].label = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
		if (v->seclabels[i].label == NULL)
		{
			logError("escaping comment failed: %s", PQerrorMessage(c));
			PQclear(res);
			PQfinish(c);
			/* XXX leak another connection? */
			exit(EXIT_FAILURE);
		}
	}

	PQclear(res);
}

void
freeViews(PQLView *v, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(v[i].obj.schemaname);
			free(v[i].obj.objectname);
			free(v[i].viewdef);
			if (v[i].reloptions)
				free(v[i].reloptions);
			if (v[i].checkoption)
				free(v[i].checkoption);
			if (v[i].comment)
				PQfreemem(v[i].comment);
			free(v[i].owner);

			/* security labels */
			for (j = 0; j < v[i].nseclabels; j++)
			{
				free(v[i].seclabels[j].provider);
				PQfreemem(v[i].seclabels[j].label);
			}

			if (v[i].seclabels)
				free(v[i].seclabels);
		}

		free(v);
	}
}

void
dumpDropView(FILE *output, PQLView *v)
{
	char	*schema = formatObjectIdentifier(v->obj.schemaname);
	char	*viewname = formatObjectIdentifier(v->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP VIEW %s.%s;", schema, viewname);

	free(schema);
	free(viewname);
}

void
dumpCreateView(FILE *output, PQLView *v)
{
	char	*schema = formatObjectIdentifier(v->obj.schemaname);
	char	*viewname = formatObjectIdentifier(v->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE VIEW %s.%s", schema, viewname);

	/* reloptions */
	if (v->reloptions != NULL)
		fprintf(output, " WITH (%s)", v->reloptions);

	fprintf(output, " AS\n%s", v->viewdef);

	if (v->checkoption != NULL)
		fprintf(output, "\n WITH %s CHECK OPTION", v->checkoption);

	fprintf(output, ";");

	/* comment */
	if (options.comment && v->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON VIEW %s.%s IS %s;", schema, viewname, v->comment);
	}

	/* security labels */
	if (options.securitylabels && v->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < v->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS %s;",
					v->seclabels[i].provider,
					schema,
					viewname,
					v->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(v->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s OWNER TO %s;", schema, viewname, owner);

		free(owner);
	}

	free(schema);
	free(viewname);
}

void
dumpAlterView(FILE *output, PQLView *a, PQLView *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*viewname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*viewname2 = formatObjectIdentifier(b->obj.objectname);

	/* check option */
	if (a->checkoption == NULL && b->checkoption != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s SET (check_option=%s);", schema2, viewname2,
				b->checkoption);
	}
	else if (a->checkoption != NULL && b->checkoption == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s RESET (check_option);", schema2, viewname2);
	}
	else if (a->checkoption != NULL && b->checkoption != NULL &&
			 strcmp(a->checkoption, b->checkoption) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s SET (check_option=%s);", schema2, viewname2,
				b->checkoption);
	}

	/* reloptions */
	if (a->reloptions == NULL && b->reloptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s SET (%s);", schema2, viewname2,
				b->reloptions);
	}
	else if (a->reloptions != NULL && b->reloptions == NULL)
	{
		stringList	*rlist;

		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE,
									false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER VIEW %s.%s RESET (%s);", schema2, viewname2, resetlist);

			free(resetlist);
			freeStringList(rlist);
		}
	}
	else if (a->reloptions != NULL && b->reloptions != NULL &&
			 strcmp(a->reloptions, b->reloptions) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE,
									false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER VIEW %s.%s RESET (%s);", schema2, viewname2, resetlist);

			free(resetlist);
			freeStringList(rlist);
		}

		/*
		 * Include intersection between option sets. However, exclude options
		 * that don't change.
		 */
		ilist = setOperationOptions(a->reloptions, b->reloptions, PGQ_INTERSECT, true,
									true);
		if (ilist)
		{
			char	*setlist;

			setlist = printOptions(ilist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER VIEW %s.%s SET (%s);", schema2, viewname2, setlist);

			free(setlist);
			freeStringList(ilist);
		}

		/*
		 * Set options that are only presented in the second set.
		 */
		slist = setOperationOptions(b->reloptions, a->reloptions, PGQ_SETDIFFERENCE,
									true, true);
		if (slist)
		{
			char	*setlist;

			setlist = printOptions(slist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER VIEW %s.%s SET (%s);", schema2, viewname2, setlist);

			free(setlist);
			freeStringList(slist);
		}
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON VIEW %s.%s IS %s;", schema2, viewname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON VIEW %s.%s IS NULL;", schema2, viewname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS %s;",
						b->seclabels[i].provider,
						schema2,
						viewname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						viewname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS %s;",
							b->seclabels[j].provider,
							schema2,
							viewname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							viewname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS %s;",
								b->seclabels[j].provider,
								schema2,
								viewname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							viewname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON VIEW %s.%s IS %s;",
							b->seclabels[j].provider,
							schema2,
							viewname2,
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
			fprintf(output, "ALTER VIEW %s.%s OWNER TO %s;", schema2, viewname2, owner);

			free(owner);
		}
	}

	free(schema1);
	free(viewname1);
	free(schema2);
	free(viewname2);
}
