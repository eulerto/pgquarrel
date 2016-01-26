#include "conversion.h"

/*
 * CREATE CONVERSION
 * DROP CONVERSION
 * ALTER CONVERSION
 * COMMENT ON CONVERSION
 *
 * TODO
 *
 * ALTER CONVERSION ... RENAME TO
 * ALTER CONVERSION ... SET SCHEMA
 */

PQLConversion *
getConversions(PGconn *c, int *n)
{
	char			*query = NULL;
	int				nquery = PGQQRYLEN;
	PQLConversion	*d;
	PGresult		*res;
	int				i;
	int				r;

	logNoise("conversion: server version: %d", PQserverVersion(c));

	do {
		query = (char *) malloc(nquery * sizeof(char));

		r = snprintf(query, nquery,
				"SELECT c.oid, n.nspname as conschema, c.conname, pg_encoding_to_char(conforencoding) AS conforencoding, pg_encoding_to_char(contoencoding) AS contoencoding, conproc, condefault, obj_description(c.oid, 'pg_conversion') AS description, pg_get_userbyid(c.conowner) AS conowner FROM pg_conversion c LEFT JOIN pg_namespace n ON (c.connamespace = n.oid) LEFT JOIN (pg_description d INNER JOIN pg_class x ON (x.oid = d.classoid AND x.relname = 'pg_conversion')) ON (d.objoid = c.oid) WHERE c.oid >= %u ORDER BY n.nspname, c.conname", PGQ_FIRST_USER_OID);

		if (r < nquery)
			break;

		logNoise("query size: required (%u) ; initial (%u)", r, nquery);
		nquery = r + 1;	/* make enough room for query */
		free(query);
	} while (true);

	res = PQexec(c, query);

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

		logDebug("conversion %s.%s ; %s => %s", d[i].obj.schemaname, d[i].obj.objectname, d[i].forencoding, d[i].toencoding);
	}

	PQclear(res);

	return d;
}

void
dumpCreateConversion(FILE *output, PQLConversion c)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE%s CONVERSION %s.%s FOR '%s' TO '%s' FROM %s",
				((c.convdefault) ? " DEFAULT" : ""),
				formatObjectIdentifier(c.obj.schemaname),
				formatObjectIdentifier(c.obj.objectname),
				c.forencoding,
				c.toencoding,
				c.funcname);
	fprintf(output, ";");

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER CONVERSION %s.%s OWNER TO %s;",
				formatObjectIdentifier(c.obj.schemaname),
				formatObjectIdentifier(c.obj.objectname),
				c.owner);
	}
}

void
dumpDropConversion(FILE *output, PQLConversion c)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP CONVERSION %s.%s;",
					formatObjectIdentifier(c.obj.schemaname),
					formatObjectIdentifier(c.obj.objectname));
}

void
dumpAlterConversion(FILE *output, PQLConversion a, PQLConversion b)
{
	/* owner */
	if (options.owner)
	{
		if (strcmp(a.owner, b.owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER CONVERSION %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
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
			fprintf(output, "COMMENT ON CONVERSION %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CONVERSION %s.%s IS NULL;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
	}
}
