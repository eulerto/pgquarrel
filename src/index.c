/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * index.c
 *     Generate INDEX commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE INDEX
 * DROP INDEX
 * ALTER INDEX
 * COMMENT ON INDEX
 *
 * TODO
 *
 * ALTER INDEX ... RENAME TO
 * ALTER INDEX ... SET TABLESPACE
 * CREATE INDEX ... [ NULLS [ NOT ] DISTINCT ]
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "index.h"


PQLIndex *
getIndexes(PGconn *c, int *n)
{
	PQLIndex	*i;
	char		*query;
	PGresult	*res;
	int			k;

	logNoise("index: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 100000 && !options.tablepartition){
		// THIS QUERY IS OPTIMIZED ONLY FOR DISCOVERING INDEXES WHICH DOES NOT BELONG PARTITIONS 
		query = psprintf("SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, pg_get_indexdef(c.oid) AS indexdef, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) INNER JOIN pg_index i ON (i.indexrelid = c.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) LEFT JOIN pg_class pt ON NOT c.relispartition AND i.indrelid=pt.oid WHERE c.relkind = 'i' AND NOT c.relispartition AND NOT pt.relispartition AND nspname !~ '^pg_' AND nspname <> 'information_schema' %s%s AND NOT indisprimary ORDER BY nspname, c.relname", include_schema_str, exclude_schema_str);
	}
	else {
		query = psprintf("SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, pg_get_indexdef(c.oid) AS indexdef, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) INNER JOIN pg_index i ON (i.indexrelid = c.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'i' AND nspname !~ '^pg_' AND nspname <> 'information_schema' %s%s AND NOT indisprimary ORDER BY nspname, relname", include_schema_str, exclude_schema_str);
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
		i = (PQLIndex *) malloc(*n * sizeof(PQLIndex));
	else
		i = NULL;

	logDebug("number of indexes in server: %d", *n);

	for (k = 0; k < *n; k++)
	{
		char	*withoutescape;

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
		{
			withoutescape = PQgetvalue(res, k, PQfnumber(res, "description"));
			i[k].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (i[k].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		logDebug("index \"%s\".\"%s\"", i[k].obj.schemaname, i[k].obj.objectname);
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
				PQfreemem(i[j].comment);
		}

		free(i);
	}
}

void
dumpDropIndex(FILE *output, PQLIndex *i)
{
	char	*schema = formatObjectIdentifier(i->obj.schemaname);
	char	*idxname = formatObjectIdentifier(i->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP INDEX %s.%s;", schema, idxname);

	free(schema);
	free(idxname);
}

void
dumpCreateIndex(FILE *output, PQLIndex *i)
{
	char	*schema = formatObjectIdentifier(i->obj.schemaname);
	char	*idxname = formatObjectIdentifier(i->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "%s;", i->indexdef);

	/* comment */
	if (options.comment && i->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON INDEX %s.%s IS %s;",
				schema, idxname, i->comment);
	}

	free(schema);
	free(idxname);
}

void
dumpAlterIndex(FILE *output, PQLIndex *a, PQLIndex *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*idxname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*idxname2 = formatObjectIdentifier(b->obj.objectname);

	if (compareRelations(&a->obj, &b->obj) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER INDEX %s.%s RENAME TO %s;",
				schema1, idxname1, idxname2);
	}

	/*
	 * If the new tablespace is NULL, it means it is in the default tablespace
	 * (pg_default) so move it.
	 */
#ifdef _NOT_USED
	if (a->tbspcname != NULL && b->tbspcname == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER INDEX %s.%s SET TABLESPACE pg_default;",
				schema1, idxname1);
	}
	else if ((a->tbspcname == NULL && b->tbspcname != NULL) ||
			 (a->tbspcname != NULL && b->tbspcname != NULL &&
			  strcmp(a->tbspcname, b->tbspcname) != 0))
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER INDEX %s.%s SET TABLESPACE %s;",
				schema1, idxname1, b->tbspcname);
	}
#endif

	/* reloptions */
	if (a->reloptions == NULL && b->reloptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER INDEX %s.%s SET (%s);",
				schema1, idxname1, b->reloptions);
	}
	else if (a->reloptions != NULL && b->reloptions == NULL)
	{
		stringList	*rlist;

		/* reset all options */
		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE,
									false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER INDEX %s.%s RESET (%s);",
					schema2, idxname2, resetlist);

			free(resetlist);
			freeStringList(rlist);
		}
	}
	else if (a->reloptions != NULL && b->reloptions != NULL &&
			 strcmp(a->reloptions, b->reloptions) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		/* reset options that are only presented in the first set */
		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE,
									false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER INDEX %s.%s RESET (%s);",
					schema2, idxname2, resetlist);

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
			fprintf(output, "ALTER INDEX %s.%s SET (%s);",
					schema2, idxname2, setlist);

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
			fprintf(output, "ALTER INDEX %s.%s SET (%s);",
					schema2, idxname2, setlist);

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
			fprintf(output, "COMMENT ON INDEX %s.%s IS %s;",
					schema2, idxname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON INDEX %s.%s IS NULL;",
					schema2, idxname2);
		}
	}

	free(schema1);
	free(idxname1);
	free(schema2);
	free(idxname2);
}
