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

		/*
		 * These values are not assigned here (see getSequenceAttributes), but
		 * default values are essential to avoid having trouble in
		 * freeSequences.
		 */
		s[i].startvalue = NULL;
		s[i].incvalue = NULL;
		s[i].minvalue = NULL;
		s[i].maxvalue = NULL;
		s[i].cache = NULL;

		/*
		 * Security labels are not assigned here (see
		 * getSequenceSecurityLabels), but default values are essential to
		 * avoid having trouble in freeSequences.
		 */
		s[i].nseclabels = 0;
		s[i].seclabels = NULL;

		logDebug("sequence \"%s\".\"%s\"", s[i].obj.schemaname, s[i].obj.objectname);
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

	do
	{
		query = (char *) malloc(nquery * sizeof(char));

		r = snprintf(query, nquery,
					 "SELECT increment_by, start_value, max_value, min_value, cache_value, is_cycled FROM %s.%s",
					 s->obj.schemaname, s->obj.objectname);

		if (r < nquery)
			break;

		logNoise("query size: required (%u) ; initial (%u)", r, nquery);
		nquery = r + 1;	/* make enough room for query */
		free(query);
	}
	while (true);

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
getSequenceSecurityLabels(PGconn *c, PQLSequence *s)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PG_VERSION_NUM < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_class' AND s.objoid = %u ORDER BY provider", s->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	s->nseclabels = PQntuples(res);
	if (s->nseclabels > 0)
		s->seclabels = (PQLSecLabel *) malloc(s->nseclabels * sizeof(PQLSecLabel));
	else
		s->seclabels = NULL;

	logDebug("number of security labels in sequence \"%s\".\"%s\": %d", s->obj.schemaname, s->obj.objectname, s->nseclabels);

	for (i = 0; i < s->nseclabels; i++)
	{
		s->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		s->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
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
			int	j;

			free(s[i].obj.schemaname);
			free(s[i].obj.objectname);
			if (s[i].comment)
				free(s[i].comment);
			free(s[i].owner);
			if (s[i].acl)
				free(s[i].acl);

			if (s[i].incvalue)
				free(s[i].incvalue);
			if (s[i].startvalue)
				free(s[i].startvalue);
			if (s[i].maxvalue)
				free(s[i].maxvalue);
			if (s[i].minvalue)
				free(s[i].minvalue);
			if (s[i].cache)
				free(s[i].cache);

			/* security labels */
			for (j = 0; j < s[i].nseclabels; j++)
			{
				free(s[i].seclabels[j].provider);
				free(s[i].seclabels[j].label);
			}

			if (s[i].seclabels)
				free(s[i].seclabels);
		}

		free(s);
	}
}

void
dumpDropSequence(FILE *output, PQLSequence *s)
{
	char	*schema = formatObjectIdentifier(s->obj.schemaname);
	char	*seqname = formatObjectIdentifier(s->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP SEQUENCE %s.%s;", schema, seqname);

	free(schema);
	free(seqname);
}

void
dumpCreateSequence(FILE *output, PQLSequence *s)
{
	char	*schema = formatObjectIdentifier(s->obj.schemaname);
	char	*seqname = formatObjectIdentifier(s->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE SEQUENCE %s.%s", schema, seqname);

	/*
	 * dump only if it is not default
	 */
	if (strcmp(s->incvalue, "1") != 0)
		fprintf(output, " INCREMENT BY %s", s->incvalue);

	/* variables used above can't be null (see getSequenceAttributes) */
	if ((atol(s->incvalue) > 0 && strcmp(s->minvalue, "1") != 0) ||
			(atol(s->incvalue) < 0 && strcmp(s->minvalue, MINIMUM_SEQUENCE_VALUE) != 0))
		fprintf(output, " MINVALUE %s", s->minvalue);

	if ((atol(s->incvalue) > 0 && strcmp(s->maxvalue, MAXIMUM_SEQUENCE_VALUE) != 0) ||
			(atol(s->incvalue) < 0 && strcmp(s->maxvalue, "-1") != 0))
		fprintf(output, " MAXVALUE %s", s->maxvalue);

	if ((atol(s->incvalue) > 0 && strcmp(s->startvalue, s->minvalue) != 0) ||
			(atol(s->incvalue) < 0 && strcmp(s->startvalue, s->maxvalue) != 0))
		fprintf(output, " START WITH %s", s->startvalue);

	if (strcmp(s->cache, "1") != 0)
		fprintf(output, " CACHE %s", s->cache);

	if (s->cycle)
		fprintf(output, " CYCLE");

	fprintf(output, ";");

	/* comment */
	if (options.comment && s->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON SEQUENCE %s.%s IS '%s';", schema, seqname, s->comment);
	}

	/* security labels */
	if (options.securitylabels && s->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < s->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS '%s';",
					s->seclabels[i].provider,
					schema,
					seqname,
					s->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SEQUENCE %s.%s OWNER TO %s;", schema, seqname, s->owner);
	}

	/* privileges */
	/* XXX second s->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_SEQUENCE, &s->obj, &s->obj, NULL, s->acl, NULL);

	free(schema);
	free(seqname);
}

void
dumpAlterSequence(FILE *output, PQLSequence *a, PQLSequence *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*seqname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*seqname2 = formatObjectIdentifier(b->obj.objectname);

	bool	printalter = true;

	if (strcmp(a->incvalue, b->incvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s", schema2, seqname2);
		}
		printalter = false;

		fprintf(output, " INCREMENT BY %s", b->incvalue);
	}

	if (strcmp(a->minvalue, b->minvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s", schema2, seqname2);
		}
		printalter = false;

		fprintf(output, " MINVALUE %s", b->minvalue);
	}

	if (strcmp(a->maxvalue, b->maxvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s", schema2, seqname2);
		}
		printalter = false;

		fprintf(output, " MAXVALUE %s", b->maxvalue);
	}

	if (strcmp(a->startvalue, b->startvalue) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s", schema2, seqname2);
		}
		printalter = false;

		fprintf(output, " START WITH %s RESTART WITH %s", b->startvalue, b->startvalue);
	}

	if (strcmp(a->cache, b->cache) != 0)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s", schema2, seqname2);
		}
		printalter = false;

		fprintf(output, " CACHE %s", b->cache);
	}

	if (a->cycle != b->cycle)
	{
		if (printalter)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SEQUENCE %s.%s", schema2, seqname2);
		}
		printalter = false;

		if (b->cycle)
			fprintf(output, " CYCLE");
		else
			fprintf(output, " NO CYCLE");
	}

	if (!printalter)
		fprintf(output, ";");

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SEQUENCE %s.%s IS '%s';", schema2, seqname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SEQUENCE %s.%s IS NULL;", schema2, seqname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS '%s';",
						b->seclabels[i].provider,
						schema2,
						seqname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						seqname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							seqname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							seqname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS '%s';",
								b->seclabels[j].provider,
								schema2,
								seqname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							seqname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SEQUENCE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							seqname2,
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
			fprintf(output, "ALTER SEQUENCE %s.%s OWNER TO %s;", schema2, seqname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_SEQUENCE, &a->obj, &b->obj, a->acl, b->acl, NULL);
	}

	free(schema1);
	free(seqname1);
	free(schema2);
	free(seqname2);
}
