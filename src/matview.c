/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * matview.c
 *     Generate MATERIALIZED VIEW commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE MATERIALIZED VIEW
 * DROP MATERIALIZED VIEW
 * ALTER MATERIALIZED VIEW
 *
 * TODO
 *
 * CREATE MATERIALIZED VIEW ... TABLESPACE
 * ALTER MATERIALIZED VIEW ... CLUSTER ON
 * ALTER MATERIALIZED VIEW ... SET WITHOUT CLUSTER
 * ALTER MATERIALIZED VIEW ... RENAME TO
 * ALTER MATERIALIZED VIEW ... RENAME COLUMN ... TO
 * ALTER MATERIALIZED VIEW ... SET SCHEMA
 * ALTER MATERIALIZED VIEW ... SET TABLESPACE
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "matview.h"


static void dumpAlterColumnSetStatistics(FILE *output, PQLMaterializedView *a,
		int i, bool force);
static void dumpAlterColumnSetStorage(FILE *output, PQLMaterializedView *a,
									  int i, bool force);
static void dumpAlterColumnSetOptions(FILE *output, PQLMaterializedView *a,
									  PQLMaterializedView *b, int i);

PQLMaterializedView *
getMaterializedViews(PGconn *c, int *n)
{
	PQLMaterializedView		*v;
	PGresult	*res;
	int			i;

	logNoise("materialized view: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 90300)
	{
		logWarning("ignoring materialized views because server does not support it");
		return NULL;
	}

	res = PQexec(c,
				 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, pg_get_viewdef(c.oid) AS viewdef, array_to_string(c.reloptions, ', ') AS reloptions, relispopulated, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'm' AND nspname !~ '^pg_' AND nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, relname");

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
		v = (PQLMaterializedView *) malloc(*n * sizeof(PQLMaterializedView));
	else
		v = NULL;

	logDebug("number of materialized views in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		int	len;

		v[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		v[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		v[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		if (PQgetisnull(res, i, PQfnumber(res, "tablespacename")))
			v[i].tbspcname = NULL;
		else
			v[i].tbspcname = strdup(PQgetvalue(res, i, PQfnumber(res, "tablespacename")));
		v[i].populated = (PQgetvalue(res, i, PQfnumber(res,
									 "relispopulated"))[0] == 't');

		/* FIXME don't load it only iff view will be DROPped */
		len = PQgetlength(res, i, PQfnumber(res, "viewdef"));
		/* allocate only len because semicolon will be stripped */
		v[i].viewdef = (char *) malloc(len * sizeof(char));
		strncpy(v[i].viewdef, PQgetvalue(res, i, PQfnumber(res, "viewdef")), len - 1);
		v[i].viewdef[len - 1] = '\0';

		if (PQgetisnull(res, i, PQfnumber(res, "reloptions")))
			v[i].reloptions = NULL;
		else
			v[i].reloptions = strdup(PQgetvalue(res, i, PQfnumber(res, "reloptions")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			v[i].comment = NULL;
		else
			v[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		v[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "relowner")));

		/*
		 * Security labels are not assigned here (see
		 * getMaterializedViewSecurityLabels), but default values are essential
		 * to avoid having trouble in freeMaterializedViews.
		 */
		v[i].nseclabels = 0;
		v[i].seclabels = NULL;

		logDebug("materialized view \"%s\".\"%s\"", v[i].obj.schemaname, v[i].obj.objectname);

		/*
		 * These values are not assigned here (see
		 * getMaterializedViewAttributes), but default values are essential to
		 * avoid having trouble in freeMaterializedViews.
		 */
		v[i].nattributes = 0;
		v[i].attributes = NULL;

		if (v[i].reloptions)
			logDebug("materialized view \"%s\".\"%s\": reloptions: %s", v[i].obj.schemaname, v[i].obj.objectname, v[i].reloptions);
		else
			logDebug("materialized view \"%s\".\"%s\": no reloptions", v[i].obj.schemaname, v[i].obj.objectname);
	}

	PQclear(res);

	return v;
}

void
getMaterializedViewAttributes(PGconn *c, PQLMaterializedView *v)
{
	char		*query = NULL;
	int			nquery = 0;
	PGresult	*res;
	int			i;

	/* determine how many characters will be written by snprintf */
	/* FIXME attcollation (9.1)? */
	nquery = snprintf(query, nquery,
				 "SELECT a.attnum, a.attname, a.attstattarget, a.attstorage, CASE WHEN t.typstorage <> a.attstorage THEN FALSE ELSE TRUE END AS defstorage, array_to_string(attoptions, ', ') AS attoptions FROM pg_attribute a LEFT JOIN pg_type t ON (a.atttypid = t.oid) WHERE a.attrelid = %u AND a.attnum > 0 AND attisdropped IS FALSE ORDER BY a.attname",
				 v->obj.oid);

	nquery++;
	query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
	snprintf(query, nquery,
			 "SELECT a.attnum, a.attname, a.attstattarget, a.attstorage, CASE WHEN t.typstorage <> a.attstorage THEN FALSE ELSE TRUE END AS defstorage, array_to_string(attoptions, ', ') AS attoptions FROM pg_attribute a LEFT JOIN pg_type t ON (a.atttypid = t.oid) WHERE a.attrelid = %u AND a.attnum > 0 AND attisdropped IS FALSE ORDER BY a.attname",
			 v->obj.oid);

	logNoise("materialized view: query size: %d ; query: %s", nquery, query);

	res = PQexec(c, query);

	free(query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	v->nattributes = PQntuples(res);
	if (v->nattributes > 0)
		v->attributes = (PQLAttribute *) malloc(v->nattributes * sizeof(PQLAttribute));
	else
		v->attributes = NULL;

	logDebug("number of attributes in materialized view \"%s\".\"%s\": %d", v->obj.schemaname, v->obj.objectname, v->nattributes);

	for (i = 0; i < v->nattributes; i++)
	{
		char	storage;

		v->attributes[i].attnum = strtoul(PQgetvalue(res, i, PQfnumber(res, "attnum")),
										  NULL, 10);
		v->attributes[i].attname = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "attname")));

		/* those fields are not used in materialized views */
		v->attributes[i].attnotnull = false;
		v->attributes[i].atttypname = NULL;
		v->attributes[i].attdefexpr = NULL;
		v->attributes[i].attcollation = NULL;
		v->attributes[i].comment = NULL;

		/* statistics target */
		v->attributes[i].attstattarget = atoi(PQgetvalue(res, i, PQfnumber(res,
											  "attstattarget")));

		/* storage */
		storage = PQgetvalue(res, i, PQfnumber(res, "attstorage"))[0];
		switch (storage)
		{
			case 'p':
				v->attributes[i].attstorage = strdup("PLAIN");
				break;
			case 'e':
				v->attributes[i].attstorage = strdup("EXTERNAL");
				break;
			case 'm':
				v->attributes[i].attstorage = strdup("MAIN");
				break;
			case 'x':
				v->attributes[i].attstorage = strdup("EXTENDED");
				break;
			default:
				v->attributes[i].attstorage = NULL;
				break;
		}
		v->attributes[i].defstorage = (PQgetvalue(res, i, PQfnumber(res,
									   "defstorage"))[0] == 't');

		/* attribute options */
		if (PQgetisnull(res, i, PQfnumber(res, "attoptions")))
			v->attributes[i].attoptions = NULL;
		else
			v->attributes[i].attoptions = strdup(PQgetvalue(res, i, PQfnumber(res,
												 "attoptions")));
	}

	PQclear(res);
}

void
getMaterializedViewSecurityLabels(PGconn *c, PQLMaterializedView *v)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_class' AND s.objoid = %u ORDER BY provider", v->obj.oid);

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

	logDebug("number of security labels in materialized view \"%s\".\"%s\": %d", v->obj.schemaname, v->obj.objectname, v->nseclabels);

	for (i = 0; i < v->nseclabels; i++)
	{
		v->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		v->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

void
freeMaterializedViews(PQLMaterializedView *v, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(v[i].obj.schemaname);
			free(v[i].obj.objectname);
			if (v[i].tbspcname)
				free(v[i].tbspcname);
			free(v[i].viewdef);
			if (v[i].reloptions)
				free(v[i].reloptions);
			if (v[i].comment)
				free(v[i].comment);
			free(v[i].owner);

			/* security labels */
			for (j = 0; j < v[i].nseclabels; j++)
			{
				free(v[i].seclabels[j].provider);
				free(v[i].seclabels[j].label);
			}

			if (v[i].seclabels)
				free(v[i].seclabels);

			/* attributes */
			for (j = 0; j < v[i].nattributes; j++)
			{
				free(v[i].attributes[j].attname);
				if (v[i].attributes[j].attstorage)
					free(v[i].attributes[j].attstorage);
				if (v[i].attributes[j].attoptions)
					free(v[i].attributes[j].attoptions);
			}

			if (v[i].attributes)
				free(v[i].attributes);
		}

		free(v);
	}
}

void
dumpDropMaterializedView(FILE *output, PQLMaterializedView *v)
{
	char	*schema = formatObjectIdentifier(v->obj.schemaname);
	char	*matvname = formatObjectIdentifier(v->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP MATERIALIZED VIEW %s.%s;", schema, matvname);

	free(schema);
	free(matvname);
}

void
dumpCreateMaterializedView(FILE *output, PQLMaterializedView *v)
{
	char	*schema = formatObjectIdentifier(v->obj.schemaname);
	char	*matvname = formatObjectIdentifier(v->obj.objectname);

	int	i;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE MATERIALIZED VIEW %s.%s", schema, matvname);

	if (v->reloptions != NULL)
		fprintf(output, " WITH (%s)", v->reloptions);

	fprintf(output, " AS\n%s", v->viewdef);

	/*
	 * create a materialized view just like a view because the content will be
	 * refreshed above.
	 */
	fprintf(output, "\n\tWITH NO DATA");
	fprintf(output, ";");

	fprintf(output, "\n\n");
	fprintf(output, "REFRESH MATERIALIZED VIEW %s.%s;", schema, matvname);

	/* statistics target */
	for (i = 0; i < v->nattributes; i++)
	{
		dumpAlterColumnSetStatistics(output, v, i, false);
		dumpAlterColumnSetStorage(output, v, i, false);
	}

	/* comment */
	if (options.comment && v->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON MATERIALIZED VIEW %s.%s IS '%s';", schema, matvname, v->comment);
	}

	/* security labels */
	if (options.securitylabels && v->nseclabels > 0)
	{
		for (i = 0; i < v->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS '%s';",
					v->seclabels[i].provider,
					schema,
					matvname,
					v->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s OWNER TO %s;", schema, matvname, v->owner);
	}

	free(schema);
	free(matvname);
}

static void
dumpAlterColumnSetStatistics(FILE *output, PQLMaterializedView *a, int i,
							 bool force)
{
	char	*schema = formatObjectIdentifier(a->obj.schemaname);
	char	*matvname = formatObjectIdentifier(a->obj.objectname);

	if (a->attributes[i].attstattarget != -1 || force)
	{
		fprintf(output, "\n\n");
		fprintf(output,
				"ALTER MATERIALIZED VIEW %s.%s ALTER COLUMN %s SET STATISTICS %d;",
				schema,
				matvname,
				a->attributes[i].attname,
				a->attributes[i].attstattarget);
	}

	free(schema);
	free(matvname);
}

static void
dumpAlterColumnSetStorage(FILE *output, PQLMaterializedView *a, int i,
						  bool force)
{
	char	*schema = formatObjectIdentifier(a->obj.schemaname);
	char	*matvname = formatObjectIdentifier(a->obj.objectname);

	if (!a->attributes[i].defstorage || force)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s ALTER COLUMN %s SET STORAGE %s;",
				schema,
				matvname,
				a->attributes[i].attname,
				a->attributes[i].attstorage);
	}

	free(schema);
	free(matvname);
}

/*
 * Set attribute options if needed
 */
static void
dumpAlterColumnSetOptions(FILE *output, PQLMaterializedView *a,
						  PQLMaterializedView *b, int i)
{
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*matvname2 = formatObjectIdentifier(b->obj.objectname);

	if (a->attributes[i].attoptions == NULL && b->attributes[i].attoptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s ALTER COLUMN %s SET (%s);",
				schema2,
				matvname2,
				b->attributes[i].attname,
				b->attributes[i].attoptions);
	}
	else if (a->attributes[i].attoptions != NULL &&
			 b->attributes[i].attoptions == NULL)
	{
		stringList	*rlist;

		/* reset all options */
		rlist = setOperationOptions(a->attributes[i].attoptions, b->attributes[i].attoptions,
							   PGQ_SETDIFFERENCE, false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s ALTER COLUMN %s RESET (%s);",
					schema2,
					matvname2,
					b->attributes[i].attname,
					resetlist);

			free(resetlist);
			freeStringList(rlist);
		}
	}
	else if (a->attributes[i].attoptions != NULL &&
			 b->attributes[i].attoptions != NULL &&
			 strcmp(a->attributes[i].attoptions, b->attributes[i].attoptions) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		/* reset options that are only presented in the first set */
		rlist = setOperationOptions(a->attributes[i].attoptions, b->attributes[i].attoptions,
							   PGQ_SETDIFFERENCE, false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s ALTER COLUMN %s RESET (%s);",
					schema2,
					matvname2,
					b->attributes[i].attname,
					resetlist);

			free(resetlist);
			freeStringList(rlist);
		}

		/*
		 * Include intersection between option sets. However, exclude
		 * options that don't change.
		 */
		ilist = setOperationOptions(a->attributes[i].attoptions, b->attributes[i].attoptions, PGQ_INTERSECT, true, true);
		if (ilist)
		{
			char	*setlist;

			setlist = printOptions(ilist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s ALTER COLUMN %s SET (%s);",
					schema2,
					matvname2,
					b->attributes[i].attname,
					setlist);

			free(setlist);
			freeStringList(ilist);
		}

		/*
		 * Set options that are only presented in the second set.
		 */
		slist = setOperationOptions(b->attributes[i].attoptions, a->attributes[i].attoptions, PGQ_SETDIFFERENCE, true, true);
		if (slist)
		{
			char	*setlist;

			setlist = printOptions(slist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s ALTER COLUMN %s SET (%s);",
					schema2,
					matvname2,
					b->attributes[i].attname,
					setlist);

			free(setlist);
			freeStringList(slist);
		}
	}

	free(schema2);
	free(matvname2);
}

void
dumpAlterMaterializedView(FILE *output, PQLMaterializedView *a,
						  PQLMaterializedView *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*matvname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*matvname2 = formatObjectIdentifier(b->obj.objectname);

	int i;

	/* the attributes are sorted by name */
	for (i = 0; i < a->nattributes; i++)
	{
		/* do attribute options change? */
		dumpAlterColumnSetOptions(output, a, b, i);

		/* column statistics changed */
		if (a->attributes[i].attstattarget != b->attributes[i].attstattarget)
			dumpAlterColumnSetStatistics(output, b, i, true);

		/* storage changed */
		if (a->attributes[i].defstorage != b->attributes[i].defstorage)
			dumpAlterColumnSetStorage(output, b, i, true);
	}

	/* reloptions */
	if (a->reloptions == NULL && b->reloptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s SET (%s);",
				schema2,
				matvname2,
				b->reloptions);
	}
	else if (a->reloptions != NULL && b->reloptions == NULL)
	{
		stringList	*rlist;

		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE, false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s RESET (%s);",
					schema2,
					matvname2,
					resetlist);

			free(resetlist);
			freeStringList(rlist);
		}
	}
	else if (a->reloptions != NULL && b->reloptions != NULL &&
			 strcmp(a->reloptions, b->reloptions) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		/* reset options that are only presented in the first set */
		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE, false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s RESET (%s);",
					schema2,
					matvname2,
					resetlist);

			free(resetlist);
			freeStringList(rlist);
		}

		/*
		 * Include intersection between option sets. However, exclude options
		 * that don't change.
		 */
		ilist = setOperationOptions(a->reloptions, b->reloptions, PGQ_INTERSECT, true, true);
		if (ilist)
		{
			char	*setlist;

			setlist = printOptions(ilist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s SET (%s);",
					schema2,
					matvname2,
					setlist);

			free(setlist);
			freeStringList(ilist);
		}

		/*
		 * Set options that are only presented in the second set.
		 */
		slist = setOperationOptions(b->reloptions, a->reloptions, PGQ_SETDIFFERENCE, true, true);
		if (slist)
		{
			char	*setlist;

			setlist = printOptions(slist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s SET (%s);",
					schema2,
					matvname2,
					setlist);

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
			fprintf(output, "COMMENT ON MATERIALIZED VIEW %s.%s IS '%s';",
					schema2,
					matvname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON MATERIALIZED VIEW %s.%s IS NULL;",
					schema2,
					matvname2);
		}
	}

	/* security labels */
	if (options.securitylabels)
	{
		if (a->seclabels == NULL && b->seclabels != NULL)
		{
			for (i = 0; i < b->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS '%s';",
						b->seclabels[i].provider,
						schema2,
						matvname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						matvname1);
			}
		}
		else if (a->seclabels != NULL && b->seclabels != NULL)
		{
			int	j;

			i = j = 0;
			while (i < a->nseclabels || j < b->nseclabels)
			{
				if (i == a->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							matvname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							matvname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS '%s';",
								b->seclabels[j].provider,
								schema2,
								matvname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							matvname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON MATERIALIZED VIEW %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							matvname2,
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
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s OWNER TO %s;",
					schema2,
					matvname2,
					b->owner);
		}
	}

	free(schema1);
	free(matvname1);
	free(schema2);
	free(matvname2);
}
