#include "view.h"

/*
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
 */

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
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid) AS viewdef, array_to_string(array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded'), ', ') AS reloptions, CASE WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'v' AND nspname !~ '^pg_' AND nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, relname");
	else
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, pg_get_viewdef(c.oid) AS viewdef, array_to_string(c.reloptions, ', ') AS reloptions, CASE WHEN 'check_option=local' = ANY(c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY(c.reloptions) THEN 'CASCADED'::text ELSE NULL END AS checkoption, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'v' AND nspname !~ '^pg_' AND nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY nspname, relname");

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
			v[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		v[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "relowner")));

		logDebug("view %s.%s", formatObjectIdentifier(v[i].obj.schemaname),
				 formatObjectIdentifier(v[i].obj.objectname));
	}

	PQclear(res);

	return v;
}

void
freeViews(PQLView *v, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(v[i].obj.schemaname);
			free(v[i].obj.objectname);
			free(v[i].viewdef);
			if (v[i].reloptions)
				free(v[i].reloptions);
			if (v[i].checkoption)
				free(v[i].checkoption);
			if (v[i].comment)
				free(v[i].comment);
			free(v[i].owner);
		}

		free(v);
	}
}

void
dumpDropView(FILE *output, PQLView v)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP VIEW %s.%s;",
			formatObjectIdentifier(v.obj.schemaname),
			formatObjectIdentifier(v.obj.objectname));
}

void
dumpCreateView(FILE *output, PQLView v)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE VIEW %s.%s", formatObjectIdentifier(v.obj.schemaname),
			formatObjectIdentifier(v.obj.objectname));

	/* reloptions */
	if (v.reloptions != NULL)
		fprintf(output, " WITH (%s)", v.reloptions);

	fprintf(output, " AS\n%s", v.viewdef);

	if (v.checkoption != NULL)
		fprintf(output, "\n WITH %s CHECK OPTION", v.checkoption);

	fprintf(output, ";");

	/* comment */
	if (options.comment && v.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON VIEW %s.%s IS '%s';",
				formatObjectIdentifier(v.obj.schemaname),
				formatObjectIdentifier(v.obj.objectname),
				v.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s OWNER TO %s;",
				formatObjectIdentifier(v.obj.schemaname),
				formatObjectIdentifier(v.obj.objectname),
				v.owner);
	}
}

void
dumpAlterView(FILE *output, PQLView a, PQLView b)
{
	/* check option */
	if (a.checkoption == NULL && b.checkoption != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s SET (check_option=%s)",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname),
				b.checkoption);
		fprintf(output, ";");
	}
	else if (a.checkoption != NULL && b.checkoption == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s RESET (check_option);",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname));
	}
	else if (a.checkoption != NULL && b.checkoption != NULL &&
			 strcmp(a.checkoption, b.checkoption) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s SET (check_option=%s)",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname),
				b.checkoption);
		fprintf(output, ";");
	}

	/* reloptions */
	if (a.reloptions == NULL && b.reloptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER VIEW %s.%s SET (%s)",
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
			fprintf(output, "\n\n");
			fprintf(output, "ALTER VIEW %s.%s RESET (%s)",
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
			fprintf(output, "ALTER VIEW %s.%s SET (%s)",
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
			fprintf(output, "ALTER VIEW %s.%s RESET (%s)",
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
			fprintf(output, "COMMENT ON VIEW %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON VIEW %s.%s IS NULL;",
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
			fprintf(output, "ALTER VIEW %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}
}
