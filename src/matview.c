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
 * ALTER MATERIALIZED VIEW ... CLUSTER ON
 * ALTER MATERIALIZED VIEW ... SET WITHOUT CLUSTER
 * ALTER MATERIALIZED VIEW ... SET TABLESPACE
 */

PQLMaterializedView *
getMaterializedViews(PGconn *c, int *n)
{
	PQLMaterializedView		*v;
	PGresult	*res;
	int			i;

	logNoise("materialized view: server version: %d", PQserverVersion(c));

	/*
	 * FIXME exclude check_option from reloptions.
	 * check_option is new in 9.4
	 * array_remove() is new in 9.3
	 */
	if (PQserverVersion(c) >= 90300)
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid) AS viewdef, array_to_string(array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded'), ', ') AS reloptions, relispopulated, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'm' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");
	else
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid) AS viewdef, array_to_string(c.reloptions, ', ') AS reloptions, relispopulated, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'm' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");

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
		v[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		v[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		v[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		v[i].populated = (PQgetvalue(res, i, PQfnumber(res, "relispopulated"))[0] == 't');
		/* FIXME don't load it only iff view will be DROPped */
		v[i].viewdef = strdup(PQgetvalue(res, i, PQfnumber(res, "viewdef")));
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
	}

	PQclear(res);

	return v;
}

void
getMaterializedViewAttributes(PGconn *c, PQLMaterializedView *v)
{
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
	fprintf(output, "CREATE MATERIALIZED VIEW %s.%s (", formatObjectIdentifier(v.obj.schemaname),
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
	fprintf(output, "REFRESH MATERIALIZED VIEW %s.%s", formatObjectIdentifier(v.obj.schemaname),
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
dumpAlterMaterializedView(FILE *output, PQLMaterializedView a, PQLMaterializedView b)
{
	if ((a.reloptions == NULL && b.reloptions != NULL) ||
			(a.reloptions != NULL && b.reloptions != NULL &&
			 strcmp(a.reloptions, b.reloptions) != 0))
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s SET (%s);",
				formatObjectIdentifier(a.obj.schemaname),
				formatObjectIdentifier(a.obj.objectname), b.reloptions);
	}
#ifdef _NOT_USED
	else if (a.reloptions != NULL && b.reloptions == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER MATERIALIZED VIEW %s.%s RESET (%s);",
				formatObjectIdentifier(a.obj.schemaname),
				formatObjectIdentifier(a.obj.objectname), b.reloptions);
	}
#endif

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
