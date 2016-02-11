#include "sequence.h"

/*
 * CREATE SEQUENCE
 * DROP SEQUENCE
 * ALTER SEQUENCE
 * ALTER SEQUENCE ... OWNED BY (table.c)
 * COMMENT ON SEQUENCE
 *
 * TODO
 *
 * ALTER SEQUENCE ... OWNED BY (create sequence)
 * ALTER SEQUENCE ... RENAME TO
 * ALTER SEQUENCE ... SET SCHEMA
 */

PQLSequence *
getSequences(PGconn *c, int *n)
{
	PQLSequence		*s;
	PGresult		*res;
	int				i;

	logNoise("sequence: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				 "SELECT c.oid, n.nspname, c.relname, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE relkind = 'S' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");

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
		s = (PQLSequence *) malloc(*n * sizeof(PQLSequence));
	else
		s = NULL;

	logDebug("number of sequences in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		s[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		s[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		s[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			s[i].comment = NULL;
		else
			s[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		s[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "relowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "relacl")))
			s[i].acl = NULL;
		else
			s[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "relacl")));

		logDebug("sequence %s.%s", formatObjectIdentifier(s[i].obj.schemaname),
				 formatObjectIdentifier(s[i].obj.objectname));
	}

	PQclear(res);

	return s;
}

void
getSequenceAttributes(PGconn *c, PQLSequence *s)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			r;

	do {
		query = (char *) malloc(nquery * sizeof(char));

		r = snprintf(query, nquery,
			"SELECT increment_by, start_value, max_value, min_value, cache_value, is_cycled FROM %s.%s",
			s->obj.schemaname, s->obj.objectname);

		if (r < nquery)
			break;

		logNoise("query size: required (%u) ; initial (%u)", r, nquery);
		nquery = r + 1;	/* make enough room for query */
		free(query);
	} while (true);

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

	if (PQntuples(res) != 1)
		logError("query to get sequence information returns %d row(s) (expected 1)",
				 PQntuples(res));
	else
	{
		s->incvalue = strdup(PQgetvalue(res, 0, PQfnumber(res, "increment_by")));
		s->startvalue = strdup(PQgetvalue(res, 0, PQfnumber(res, "start_value")));
		s->maxvalue = strdup(PQgetvalue(res, 0, PQfnumber(res, "max_value")));
		s->minvalue = strdup(PQgetvalue(res, 0, PQfnumber(res, "min_value")));
		s->cache = strdup(PQgetvalue(res, 0, PQfnumber(res, "cache_value")));
		s->cycle = (PQgetvalue(res, 0, PQfnumber(res, "is_cycled"))[0] == 't');
	}

	PQclear(res);
}

void
freeSequences(PQLSequence *s, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(s[i].obj.schemaname);
			free(s[i].obj.objectname);
			if (s[i].comment)
				free(s[i].comment);
			free(s[i].owner);
			if (s[i].acl)
				free(s[i].acl);

			/* attributes */
			free(s[i].incvalue);
			free(s[i].startvalue);
			free(s[i].maxvalue);
			free(s[i].minvalue);
			free(s[i].cache);
		}

		free(s);
	}
}

void
dumpDropSequence(FILE *output, PQLSequence s)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP SEQUENCE %s.%s;",
			formatObjectIdentifier(s.obj.schemaname),
			formatObjectIdentifier(s.obj.objectname));
}

void
dumpCreateSequence(FILE *output, PQLSequence s)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE SEQUENCE %s.%s",
			formatObjectIdentifier(s.obj.schemaname),
			formatObjectIdentifier(s.obj.objectname));

	/*
	 * dump only if it is not default
	 */
	if (strcmp(s.incvalue, "1") != 0)
		fprintf(output, " INCREMENT BY %s", s.incvalue);

	if ((s.incvalue > 0 && strcmp(s.minvalue, "1") != 0) ||
			(s.incvalue < 0 && strcmp(s.minvalue, MINIMUM_SEQUENCE_VALUE) != 0))
		fprintf(output, " MINVALUE %s", s.minvalue);

	if ((s.incvalue > 0 && strcmp(s.maxvalue, MAXIMUM_SEQUENCE_VALUE) != 0) ||
			(s.incvalue < 0 && strcmp(s.maxvalue, "-1") != 0))
		fprintf(output, " MAXVALUE %s", s.maxvalue);

	if ((s.incvalue > 0 && strcmp(s.startvalue, s.minvalue) != 0) ||
			(s.incvalue < 0 && strcmp(s.startvalue, s.maxvalue) != 0))
		fprintf(output, " START WITH %s", s.startvalue);

	if (strcmp(s.cache, "1") != 0)
		fprintf(output, " CACHE %s", s.cache);

	if (s.cycle)
		fprintf(output, " CYCLE");

	fprintf(output, ";");

	/* comment */
	if (options.comment && s.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON SEQUENCE %s.%s IS '%s';",
				formatObjectIdentifier(s.obj.schemaname),
				formatObjectIdentifier(s.obj.objectname),
				s.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SEQUENCE %s.%s OWNER TO %s;",
				formatObjectIdentifier(s.obj.schemaname),
				formatObjectIdentifier(s.obj.objectname),
				s.owner);
	}

	/* privileges */
	/* XXX second s.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_SEQUENCE, s.obj, s.obj, NULL, s.acl, NULL);
}

void
dumpAlterSequence(FILE *output, PQLSequence a, PQLSequence b)
{
	bool	printalter = true;

	if (strcmp(a.incvalue, b.incvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
		printalter = false;

		fprintf(output, " INCREMENT BY %s", b.incvalue);
	}

	if (strcmp(a.minvalue, b.minvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
		printalter = false;

		fprintf(output, " MINVALUE %s", b.minvalue);
	}

	if (strcmp(a.maxvalue, b.maxvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
		printalter = false;

		fprintf(output, " MAXVALUE %s", b.maxvalue);
	}

	if (strcmp(a.startvalue, b.startvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
		printalter = false;

		fprintf(output, " START WITH %s RESTART WITH %s", b.startvalue, b.startvalue);
	}

	if (strcmp(a.cache, b.cache) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
		printalter = false;

		fprintf(output, " CACHE %s", b.cache);
	}

	if (a.cycle != b.cycle)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
		printalter = false;

		if (b.cycle)
			fprintf(output, " CYCLE");
		else
			fprintf(output, " NO CYCLE");
	}

	if (!printalter)
		fprintf(output, ";");

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SEQUENCE %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SEQUENCE %s.%s IS NULL;",
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
			fprintf(output, "ALTER SEQUENCE %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_SEQUENCE, a.obj, b.obj, a.acl, b.acl, NULL);
	}
}
