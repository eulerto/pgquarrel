/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * collation.c
 *     Generate COLLATION commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE COLLATION
 * ALTER COLLATION
 * DROP COLLATION
 * COMMENT ON COLLATION
 *
 * TODO
 *
 * CREATE COLLATION ... PROVIDER
 * CREATE COLLATION ... VERSION
 *
 * ALTER COLLATION ... RENAME TO
 * ALTER COLLATION ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "collation.h"


PQLCollation *
getCollations(PGconn *c, int *n)
{
	char			*query;
	PQLCollation	*d;
	PGresult		*res;
	int				i;

	logNoise("collation: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring collations because server does not support it");
		return NULL;
	}
	else if (PQserverVersion(c) >= 100000)
	{
		query = psprintf("SELECT c.oid, n.nspname, collname, pg_encoding_to_char(collencoding) AS collencoding, collcollate, collctype, collprovider, pg_get_userbyid(collowner) AS collowner, obj_description(c.oid, 'pg_collation') AS description FROM pg_collation c INNER JOIN pg_namespace n ON (c.collnamespace = n.oid) WHERE c.oid >= %u AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, collname",
						  PGQ_FIRST_USER_OID);
	}
	else
	{
		query = psprintf("SELECT c.oid, n.nspname, collname, pg_encoding_to_char(collencoding) AS collencoding, collcollate, collctype, NULL AS collprovider, pg_get_userbyid(collowner) AS collowner, obj_description(c.oid, 'pg_collation') AS description FROM pg_collation c INNER JOIN pg_namespace n ON (c.collnamespace = n.oid) WHERE c.oid >= %u AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, collname",
						  PGQ_FIRST_USER_OID);
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
		d = (PQLCollation *) malloc(*n * sizeof(PQLCollation));
	else
		d = NULL;

	logDebug("number of collations in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		d[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		d[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		d[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "collname")));
		d[i].encoding = strdup(PQgetvalue(res, i, PQfnumber(res, "collencoding")));
		d[i].collate = strdup(PQgetvalue(res, i, PQfnumber(res, "collcollate")));
		d[i].ctype = strdup(PQgetvalue(res, i, PQfnumber(res, "collctype")));

		if (PQgetisnull(res, i, PQfnumber(res, "collprovider")))
			d[i].provider = NULL;
		else
			d[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "collprovider")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			d[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			d[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (d[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		d[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "collowner")));

		logDebug("collation \"%s\".\"%s\"", d[i].obj.schemaname, d[i].obj.objectname);
	}

	PQclear(res);

	return d;
}

void
freeCollations(PQLCollation *c, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(c[i].obj.schemaname);
			free(c[i].obj.objectname);
			free(c[i].encoding);
			free(c[i].collate);
			free(c[i].ctype);
			if (c[i].provider)
				free(c[i].provider);
			if (c[i].comment)
				PQfreemem(c[i].comment);
			free(c[i].owner);
		}

		free(c);
	}
}

void
dumpCreateCollation(FILE *output, PQLCollation *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*collname = formatObjectIdentifier(c->obj.objectname);

	/*
	 * All pg_conversion columns are not null, specifying collate and ctype are
	 * more flexible than locale because locale implies we can't specify
	 * collate or ctype.
	 */
	fprintf(output, "\n\n");
	fprintf(output, "CREATE COLLATION %s.%s (LC_COLLATE = '%s', LC_CTYPE = '%s'",
			schema,
			collname,
			c->collate,
			c->ctype);

	if (c->provider)
	{
		if (c->provider[0] == 'c')
			fprintf(output, ", PROVIDER = libc");
		else if (c->provider[0] == 'i')
			fprintf(output, ", PROVIDER = icu");
		/* it is not accepted on input. remove? */
		else if (c->provider[0] == 'd')
			fprintf(output, ", PROVIDER = default");
	}

	fprintf(output, ");");

	/* comment */
	if (options.comment && c->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON COLLATION %s.%s IS %s;",
				schema,
				collname,
				c->comment);
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(c->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER COLLATION %s.%s OWNER TO %s;",
				schema,
				collname,
				owner);

		free(owner);
	}

	free(schema);
	free(collname);
}

void
dumpDropCollation(FILE *output, PQLCollation *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*collname = formatObjectIdentifier(c->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP COLLATION %s.%s;",
			schema, collname);

	free(schema);
	free(collname);
}

void
dumpAlterCollation(FILE *output, PQLCollation *a, PQLCollation *b)
{
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*collname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON COLLATION %s.%s IS %s;",
					schema2,
					collname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON COLLATION %s.%s IS NULL;",
					schema2,
					collname2);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER COLLATION %s.%s OWNER TO %s;",
					schema2,
					collname2,
					owner);

			free(owner);
		}
	}

	free(schema2);
	free(collname2);
}
