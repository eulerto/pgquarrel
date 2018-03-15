/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * conversion.c
 *     Generate CONVERSION commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE CONVERSION
 * DROP CONVERSION
 * ALTER CONVERSION
 * COMMENT ON CONVERSION
 *
 * TODO
 *
 * ALTER CONVERSION ... RENAME TO
 * ALTER CONVERSION ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "conversion.h"


PQLConversion *
getConversions(PGconn *c, int *n)
{
	char			*query = NULL;
	int				nquery = 0;
	PQLConversion	*d;
	PGresult		*res;
	int				i;

	logNoise("conversion: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		/* determine how many characters will be written by snprintf */
		nquery = snprintf(query, nquery,
				 "SELECT c.oid, n.nspname as conschema, c.conname, pg_encoding_to_char(conforencoding) AS conforencoding, pg_encoding_to_char(contoencoding) AS contoencoding, conproc, condefault, obj_description(c.oid, 'pg_conversion') AS description, pg_get_userbyid(c.conowner) AS conowner FROM pg_conversion c LEFT JOIN pg_namespace n ON (c.connamespace = n.oid) WHERE c.oid >= %u AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, c.conname",
				 PGQ_FIRST_USER_OID);

		nquery++;
		query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
		snprintf(query, nquery,
				 "SELECT c.oid, n.nspname as conschema, c.conname, pg_encoding_to_char(conforencoding) AS conforencoding, pg_encoding_to_char(contoencoding) AS contoencoding, conproc, condefault, obj_description(c.oid, 'pg_conversion') AS description, pg_get_userbyid(c.conowner) AS conowner FROM pg_conversion c LEFT JOIN pg_namespace n ON (c.connamespace = n.oid) WHERE c.oid >= %u AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, c.conname",
				 PGQ_FIRST_USER_OID);

		logNoise("conversion: query size: %d ; query: %s", nquery, query);
	}
	else
	{
		/* determine how many characters will be written by snprintf */
		nquery = snprintf(query, nquery,
				 "SELECT c.oid, n.nspname as conschema, c.conname, pg_encoding_to_char(conforencoding) AS conforencoding, pg_encoding_to_char(contoencoding) AS contoencoding, conproc, condefault, obj_description(c.oid, 'pg_conversion') AS description, pg_get_userbyid(c.conowner) AS conowner FROM pg_conversion c LEFT JOIN pg_namespace n ON (c.connamespace = n.oid) WHERE c.oid >= %u ORDER BY n.nspname, c.conname",
				 PGQ_FIRST_USER_OID);

		nquery++;
		query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
		snprintf(query, nquery,
				 "SELECT c.oid, n.nspname as conschema, c.conname, pg_encoding_to_char(conforencoding) AS conforencoding, pg_encoding_to_char(contoencoding) AS contoencoding, conproc, condefault, obj_description(c.oid, 'pg_conversion') AS description, pg_get_userbyid(c.conowner) AS conowner FROM pg_conversion c LEFT JOIN pg_namespace n ON (c.connamespace = n.oid) WHERE c.oid >= %u ORDER BY n.nspname, c.conname",
				 PGQ_FIRST_USER_OID);

		logNoise("conversion: query size: %d ; query: %s", nquery, query);
	}

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

	*n = PQntuples(res);
	if (*n > 0)
		d = (PQLConversion *) malloc(*n * sizeof(PQLConversion));
	else
		d = NULL;

	logDebug("number of conversions in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		d[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		d[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "conschema")));
		d[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "conname")));
		d[i].forencoding = strdup(PQgetvalue(res, i, PQfnumber(res, "conforencoding")));
		d[i].toencoding = strdup(PQgetvalue(res, i, PQfnumber(res, "contoencoding")));
		d[i].funcname = strdup(PQgetvalue(res, i, PQfnumber(res, "conproc")));
		d[i].convdefault = (PQgetvalue(res, i, PQfnumber(res, "condefault"))[0] == 't');

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			d[i].comment = NULL;
		else
			d[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		d[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "conowner")));

		logDebug("conversion \"%s\".\"%s\" ; %s => %s", d[i].obj.schemaname,
				 d[i].obj.objectname, d[i].forencoding, d[i].toencoding);
	}

	PQclear(res);

	return d;
}

void
freeConversions(PQLConversion *c, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(c[i].obj.schemaname);
			free(c[i].obj.objectname);
			free(c[i].forencoding);
			free(c[i].toencoding);
			free(c[i].funcname);
			if (c[i].comment)
				free(c[i].comment);
			free(c[i].owner);
		}

		free(c);
	}
}

void
dumpCreateConversion(FILE *output, PQLConversion *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*convname = formatObjectIdentifier(c->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE%s CONVERSION %s.%s FOR '%s' TO '%s' FROM %s",
			((c->convdefault) ? " DEFAULT" : ""),
			schema,
			convname,
			c->forencoding,
			c->toencoding,
			c->funcname);
	fprintf(output, ";");

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER CONVERSION %s.%s OWNER TO %s;",
				schema,
				convname,
				c->owner);
	}

	free(schema);
	free(convname);
}

void
dumpDropConversion(FILE *output, PQLConversion *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*convname = formatObjectIdentifier(c->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP CONVERSION %s.%s;",
			schema,
			convname);

	free(schema);
	free(convname);
}

void
dumpAlterConversion(FILE *output, PQLConversion *a, PQLConversion *b)
{
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*convname2 = formatObjectIdentifier(b->obj.objectname);

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER CONVERSION %s.%s OWNER TO %s;",
					schema2,
					convname2,
					b->owner);
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
			fprintf(output, "COMMENT ON CONVERSION %s.%s IS '%s';",
					schema2,
					convname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CONVERSION %s.%s IS NULL;",
					schema2,
					convname2);
		}
	}

	free(schema2);
	free(convname2);
}
