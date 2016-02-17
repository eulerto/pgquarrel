#include "matview.h"

/*
 * CREATE MATERIALIZED VIEW
 * DROP MATERIALIZED VIEW
 * ALTER MATERIALIZED VIEW
 *
 * TODO
 *
 * CREATE MATERIALIZED VIEW ... TABLESPACE
 * ALTER MATERIALIZED VIEW ... ALTER COLUMN SET STATISTICS
 * ALTER MATERIALIZED VIEW ... ALTER COLUMN SET STORAGE
 * ALTER MATERIALIZED VIEW ... ALTER COLUMN ... { SET | RESET }
 * ALTER MATERIALIZED VIEW ... CLUSTER ON
 * ALTER MATERIALIZED VIEW ... SET WITHOUT CLUSTER
 * ALTER MATERIALIZED VIEW ... RENAME TO
 * ALTER MATERIALIZED VIEW ... RENAME COLUMN ... TO
 * ALTER MATERIALIZED VIEW ... SET SCHEMA
 * ALTER MATERIALIZED VIEW ... SET TABLESPACE
 */

PQLMaterializedView *
getMaterializedViews(PGconn *c, int *n)
{
	PQLMaterializedView		*v;
	PGresult	*res;
	int			i;

	logNoise("materialized view: server version: %d", PQserverVersion(c));

	/* check postgres version */
	if (PQserverVersion(c) < 90300)
	{
		logWarning("version %d does not support materialized views",
				   PQserverVersion(c));
		return NULL;
	}

	res = PQexec(c,
				 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, pg_get_viewdef(c.oid) AS viewdef, array_to_string(c.reloptions, ', ') AS reloptions, relispopulated, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'm' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");

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

		logDebug("materialized view %s.%s", formatObjectIdentifier(v[i].obj.schemaname),
				 formatObjectIdentifier(v[i].obj.objectname));

		if (v[i].reloptions)
			logDebug("materialized view %s.%s: reloptions: %s",
					 formatObjectIdentifier(v[i].obj.schemaname),
					 formatObjectIdentifier(v[i].obj.objectname),
					 v[i].reloptions);
		else
			logDebug("materialized view %s.%s: no reloptions",
					 formatObjectIdentifier(v[i].obj.schemaname),
					 formatObjectIdentifier(v[i].obj.objectname));
	}

	PQclear(res);

	return v;
}

void
freeMaterializedViews(PQLMaterializedView *v, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
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
		}

		free(v);
	}
}

void
dumpDropMaterializedView(FILE *output, PQLMaterializedView v)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP MATERIALIZED VIEW %s.%s;",
			formatObjectIdentifier(v.obj.schemaname),
			formatObjectIdentifier(v.obj.objectname));
}

void
dumpCreateMaterializedView(FILE *output, PQLMaterializedView v)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE MATERIALIZED VIEW %s.%s",
			formatObjectIdentifier(v.obj.schemaname),
			formatObjectIdentifier(v.obj.objectname));

	if (v.reloptions != NULL)
		fprintf(output, " WITH (%s)", v.reloptions);

	fprintf(output, " AS\n%s", v.viewdef);

	/*
	 * create a materialized view just like a view because the content will be
	 * refreshed above.
	 */
	fprintf(output, "\n\tWITH NO DATA");
	fprintf(output, ";");

	fprintf(output, "\n\n");
	fprintf(output, "REFRESH MATERIALIZED VIEW %s.%s",
			formatObjectIdentifier(v.obj.schemaname),
			formatObjectIdentifier(v.obj.objectname));
	fprintf(output, ";");

	/* comment */
	if (options.comment && v.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON MATERIALIZED VIEW %s.%s IS '%s';",
				formatObjectIdentifier(v.obj.schemaname),
				formatObjectIdentifier(v.obj.objectname),
				v.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s OWNER TO %s;",
				formatObjectIdentifier(v.obj.schemaname),
				formatObjectIdentifier(v.obj.objectname),
				v.owner);
	}
}

void
dumpAlterMaterializedView(FILE *output, PQLMaterializedView a,
						  PQLMaterializedView b)
{
	/* reloptions */
	if ((a.reloptions == NULL && b.reloptions != NULL))
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s SET (%s)",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname),
				b.reloptions);
		fprintf(output, ";");
	}
	else if (a.reloptions != NULL && b.reloptions != NULL &&
			 strcmp(a.reloptions, b.reloptions) != 0)
	{
		stringList	*rlist, *slist;

		rlist = diffRelOptions(a.reloptions, b.reloptions, PGQ_EXCEPT);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printRelOptions(rlist);
			fprintf(output, "\n--\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s RESET (%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					resetlist);
			fprintf(output, ";");

			free(resetlist);
			freeStringList(rlist);
		}

		/*
		 * FIXME we used to use diffRelOptions with PGQ_INTERSECT kind but it
		 * is buggy. Instead, we use all options from b. It is not wrong, but
		 * it would be nice to remove unnecessary options (e.g. same
		 * option/value).
		 */
		slist = buildRelOptions(b.reloptions);
		if (slist)
		{
			char	*setlist;

			setlist = printRelOptions(slist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s SET (%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					setlist);
			fprintf(output, ";");

			free(setlist);
			freeStringList(slist);
		}
	}
	else if (a.reloptions != NULL && b.reloptions == NULL)
	{
		stringList	*rlist;

		rlist = diffRelOptions(a.reloptions, b.reloptions, PGQ_EXCEPT);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printRelOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s RESET (%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					resetlist);
			fprintf(output, ";");

			free(resetlist);
			freeStringList(rlist);
		}
	}

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON MATERIALIZED VIEW %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON MATERIALIZED VIEW %s.%s IS NULL;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a.owner, b.owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER MATERIALIZED VIEW %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}
}
