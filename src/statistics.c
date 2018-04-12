/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * statistics.c
 *     Generate STATISTICS commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE STATISTICS
 * DROP STATISTICS
 * ALTER STATISTICS
 * COMMENT ON STATISTICS
 *
 * TODO
 *
 * ALTER STATISTICS ... RENAME TO
 * ALTER STATISTICS ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "statistics.h"


PQLStatistics *
getStatistics(PGconn *c, int *n)
{
	PQLStatistics	*s;
	PGresult		*res;
	int				i;

	logNoise("statistics: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 100000)
	{
		logWarning("ignoring statistics because server does not support it");
		return NULL;
	}

	res = PQexec(c,
				 "SELECT s.oid, n.nspname AS nspname, s.stxname AS stxname, pg_get_statisticsobjdef(s.oid) AS stxdef, obj_description(s.oid, 'pg_statistic_ext') AS description, pg_get_userbyid(s.stxowner) AS stxowner FROM pg_statistic_ext s INNER JOIN pg_namespace n ON (s.stxnamespace = n.oid) ORDER BY n.nspname, s.stxname");

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
		s = (PQLStatistics *) malloc(*n * sizeof(PQLStatistics));
	else
		s = NULL;

	logDebug("number of statistics in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		s[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		s[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		s[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "stxname")));
		s[i].stxdef = strdup(PQgetvalue(res, i, PQfnumber(res, "stxdef")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			s[i].comment = NULL;
		else
			s[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));
		s[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "stxowner")));

		logDebug("statistics \"%s\".\"%s\"", s[i].obj.schemaname, s[i].obj.objectname);
	}

	PQclear(res);

	return s;
}

void
freeStatistics(PQLStatistics *s, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(s[i].obj.schemaname);
			free(s[i].obj.objectname);
			free(s[i].stxdef);
			free(s[i].owner);
			if (s[i].comment)
				free(s[i].comment);
		}

		free(s);
	}
}

void
dumpCreateStatistics(FILE *output, PQLStatistics *s)
{
	char	*schema = formatObjectIdentifier(s->obj.schemaname);
	char	*stxname = formatObjectIdentifier(s->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "%s;", s->stxdef);

	/* comment */
	if (options.comment && s->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON STATISTICS %s.%s IS '%s';", schema, stxname,
				s->comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER STATISTICS %s.%s OWNER TO %s;", schema, stxname,
				s->owner);
	}

	free(schema);
	free(stxname);
}

void
dumpDropStatistics(FILE *output, PQLStatistics *s)
{
	char	*schema = formatObjectIdentifier(s->obj.schemaname);
	char	*stxname = formatObjectIdentifier(s->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP STATISTICS %s.%s;", schema, stxname);

	free(schema);
	free(stxname);
}

void
dumpAlterStatistics(FILE *output, PQLStatistics *a, PQLStatistics *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*stxname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*stxname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON STATISTICS %s.%s IS '%s';", schema2, stxname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON STATISTICS %s.%s IS NULL;", schema2, stxname2);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER STATISTICS %s.%s OWNER TO %s;", schema2, stxname2,
					b->owner);
		}
	}

	free(schema1);
	free(stxname1);
	free(schema2);
	free(stxname2);
}
