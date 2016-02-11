#include "index.h"

/*
 * CREATE INDEX
 * DROP INDEX
 * ALTER INDEX
 * COMMENT ON INDEX
 *
 * TODO
 *
 * ALTER INDEX ... RENAME TO
 * ALTER INDEX ... SET TABLESPACE
 */

PQLIndex *
getIndexes(PGconn *c, int *n)
{
	PQLIndex	*i;
	PGresult	*res;
	int			k;

	logNoise("index: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, pg_get_indexdef(c.oid) AS indexdef, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) INNER JOIN pg_index i ON (i.indexrelid = c.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'i' AND nspname !~ '^pg_' AND nspname <> 'information_schema' AND NOT indisprimary ORDER BY nspname, relname");

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
		i = (PQLIndex *) malloc(*n * sizeof(PQLIndex));
	else
		i = NULL;

	logDebug("number of indexes in server: %d", *n);

	for (k = 0; k < *n; k++)
	{
		i[k].obj.oid = strtoul(PQgetvalue(res, k, PQfnumber(res, "oid")), NULL, 10);
		i[k].obj.schemaname = strdup(PQgetvalue(res, k, PQfnumber(res, "nspname")));
		i[k].obj.objectname = strdup(PQgetvalue(res, k, PQfnumber(res, "relname")));
		if (PQgetisnull(res, k, PQfnumber(res, "tablespacename")))
			i[k].tbspcname = NULL;
		else
			i[k].tbspcname = strdup(PQgetvalue(res, k, PQfnumber(res, "tablespacename")));
		/* FIXME don't load it only iff index will be DROPped */
		i[k].indexdef = strdup(PQgetvalue(res, k, PQfnumber(res, "indexdef")));
		if (PQgetisnull(res, k, PQfnumber(res, "reloptions")))
			i[k].reloptions = NULL;
		else
			i[k].reloptions = strdup(PQgetvalue(res, k, PQfnumber(res, "reloptions")));
		if (PQgetisnull(res, k, PQfnumber(res, "description")))
			i[k].comment = NULL;
		else
			i[k].comment = strdup(PQgetvalue(res, k, PQfnumber(res, "description")));

		logDebug("index %s.%s", formatObjectIdentifier(i[k].obj.schemaname),
				 formatObjectIdentifier(i[k].obj.objectname));
	}

	PQclear(res);

	return i;
}

void
freeIndexes(PQLIndex *i, int n)
{
	if (n > 0)
	{
		int	j;

		for (j = 0; j < n; j++)
		{
			free(i[j].obj.schemaname);
			free(i[j].obj.objectname);
			if (i[j].tbspcname)
				free(i[j].tbspcname);
			free(i[j].indexdef);
			if (i[j].reloptions)
				free(i[j].reloptions);
			if (i[j].comment)
				free(i[j].comment);
		}

		free(i);
	}
}

void
getIndexAttributes(PGconn *c, PQLIndex *i)
{
}

void
dumpDropIndex(FILE *output, PQLIndex i)
{
		fprintf(output, "\n\n");
	fprintf(output, "DROP INDEX %s.%s;",
			formatObjectIdentifier(i.obj.schemaname),
			formatObjectIdentifier(i.obj.objectname));
}

void
dumpCreateIndex(FILE *output, PQLIndex i)
{
	fprintf(output, "\n\n");
	fprintf(output, "%s;", i.indexdef);

	/* comment */
	if (options.comment && i.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON INDEX %s.%s IS '%s';",
				formatObjectIdentifier(i.obj.schemaname),
				formatObjectIdentifier(i.obj.objectname),
				i.comment);
	}
}

void
dumpAlterIndex(FILE *output, PQLIndex a, PQLIndex b)
{
	if (compareRelations(a.obj, b.obj) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER INDEX %s.%s RENAME TO %s;",
				formatObjectIdentifier(a.obj.schemaname),
				formatObjectIdentifier(a.obj.objectname),
				formatObjectIdentifier(b.obj.objectname));
	}

	if (strcmp(a.tbspcname, b.tbspcname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER INDEX %s.%s SET TABLESPACE %s;",
				formatObjectIdentifier(a.obj.schemaname),
				formatObjectIdentifier(a.obj.objectname), b.tbspcname);
	}

	/* reloptions */
	if (a.reloptions == NULL && b.reloptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER INDEX %s.%s SET (%s);",
				formatObjectIdentifier(a.obj.schemaname),
				formatObjectIdentifier(a.obj.objectname), b.reloptions);
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
			fprintf(output, "ALTER INDEX %s.%s RESET (%s)",
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
			fprintf(output, "ALTER INDEX %s.%s SET (%s)",
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
			fprintf(output, "ALTER INDEX %s.%s RESET (%s)",
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
			fprintf(output, "COMMENT ON INDEX %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON INDEX %s.%s IS NULL;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
	}
}
