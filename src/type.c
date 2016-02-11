#include "type.h"

/*
 * CREATE TYPE
 * DROP TYPE
 * ALTER TYPE
 *
 * TODO
 *
 * ALTER TYPE ... { ADD | DROP | ALTER } ATTRIBUTE
 * ALTER TYPE ... RENAME ATTRIBUTE ... TO
 * ALTER TYPE ... RENAME TO
 * ALTER TYPE ... SET SCHEMA
 * ALTER TYPE ... ADD VALUE
 */

static void getCompositeTypeAttributes(PGconn *c, PQLCompositeType *t);
static void getEnumTypeLabels(PGconn *c, PQLEnumType *t);


PQLBaseType *
getBaseTypes(PGconn *c, int *n)
{
	PQLBaseType		*t;
	PGresult		*res;
	int				i;

	logNoise("base type: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90100)
	{
		/* typcollation is new in 9.1 */
		res = PQexec(c,
					"SELECT t.oid, n.nspname, t.typname, typlen AS length, typinput AS input, typoutput AS output, typreceive AS receive, typsend AS send, typmodin AS modin, typmodout AS modout, typanalyze AS analyze, (typcollation <> 0) as collatable, typdefault, typcategory AS category, typispreferred AS preferred, typdelim AS delimiter, typalign AS align, typstorage AS storage, typbyval AS byvalue, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'b' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else
	{
		res = PQexec(c,
					"SELECT t.oid, n.nspname, t.typname, typlen AS length, typinput AS input, typoutput AS output, typreceive AS receive, typsend AS send, typmodin AS modin, typmodout AS modout, typanalyze AS analyze, false AS collatable, typdefault, typcategory AS category, typispreferred AS preferred, typdelim AS delimiter, typalign AS align, typstorage AS storage, typbyval AS byvalue, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'b' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}

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
		t = (PQLBaseType *) malloc(*n * sizeof(PQLBaseType));
	else
		t = NULL;

	logDebug("number of base types in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		t[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "typname")));
		t[i].length = atoi(PQgetvalue(res, i, PQfnumber(res, "length")));
		t[i].input = strdup(PQgetvalue(res, i, PQfnumber(res, "input")));
		t[i].output = strdup(PQgetvalue(res, i, PQfnumber(res, "output")));
		t[i].receive = strdup(PQgetvalue(res, i, PQfnumber(res, "receive")));
		t[i].send = strdup(PQgetvalue(res, i, PQfnumber(res, "send")));
		t[i].modin = strdup(PQgetvalue(res, i, PQfnumber(res, "modin")));
		t[i].modout = strdup(PQgetvalue(res, i, PQfnumber(res, "modout")));
		t[i].analyze = strdup(PQgetvalue(res, i, PQfnumber(res, "analyze")));
		t[i].collatable = (PQgetvalue(res, i, PQfnumber(res, "collatable"))[0] == 't');
		if (PQgetisnull(res, i, PQfnumber(res, "typdefault")))
			t[i].typdefault = NULL;
		else
			t[i].typdefault = strdup(PQgetvalue(res, i, PQfnumber(res, "typdefault")));
		t[i].category = strdup(PQgetvalue(res, i, PQfnumber(res, "category")));
		t[i].preferred = (PQgetvalue(res, i, PQfnumber(res, "preferred"))[0] == 't');
		t[i].delimiter = strdup(PQgetvalue(res, i, PQfnumber(res, "delimiter")));
		t[i].align = strdup(PQgetvalue(res, i, PQfnumber(res, "align")));
		t[i].storage = strdup(PQgetvalue(res, i, PQfnumber(res, "storage")));
		t[i].byvalue = (PQgetvalue(res, i, PQfnumber(res, "byvalue"))[0] == 't');

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
			t[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		t[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "typowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "typacl")))
			t[i].acl = NULL;
		else
			t[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "typacl")));

		logDebug("base type \"%s\".\"%s\"", t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

/* TODO composite type column comments */
static void
getCompositeTypeAttributes(PGconn *c, PQLCompositeType *t)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i;
	int			r;

	do {
		query = (char *) malloc(nquery * sizeof(char));

		/* typcollation is new in 9.1 */
		if (PQserverVersion(c) >= 90100)
			r = snprintf(query, nquery,
					"SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS attdefinition, p.nspname AS collschemaname, CASE WHEN a.attcollation <> u.typcollation THEN l.collname ELSE NULL END AS collname FROM pg_type t INNER JOIN pg_attribute a ON (a.attrelid = t.typrelid) LEFT JOIN pg_type u ON (u.oid = a.atttypid) LEFT JOIN (pg_collation l LEFT JOIN pg_namespace p ON (l.collnamespace = p.oid)) ON (a.attcollation = l.oid) WHERE t.oid = %u ORDER BY a.attnum", t->obj.oid);
		else
			r = snprintf(query, nquery,
					"SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS attdefinition, NULL AS collschemaname, NULL AS collname FROM pg_type t INNER JOIN pg_attribute a ON (a.attrelid = t.typrelid) WHERE t.oid = %u ORDER BY a.attnum", t->obj.oid);

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

	t->nattributes = PQntuples(res);
	if (t->nattributes > 0)
		t->attributes = (PQLAttrCompositeType *) malloc(t->nattributes * sizeof(PQLAttrCompositeType));
	else
		t->attributes = NULL;

	logDebug("number of attributes on composite type \"%s\".\"%s\": %d", t->obj.schemaname, t->obj.objectname, t->nattributes);

	for (i = 0; i < t->nattributes; i++)
	{
		t->attributes[i].attname = strdup(PQgetvalue(res, i, PQfnumber(res, "attname")));
		t->attributes[i].typname = strdup(PQgetvalue(res, i, PQfnumber(res, "attdefinition")));
		/* collation can be NULL in 9.0 or earlier */
		if (PQgetisnull(res, i, PQfnumber(res, "collschemaname")))
			t->attributes[i].collschemaname = NULL;
		else
			t->attributes[i].collschemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "collschemaname")));
		if (PQgetisnull(res, i, PQfnumber(res, "collname")))
			t->attributes[i].collname = NULL;
		else
			t->attributes[i].collname = strdup(PQgetvalue(res, i, PQfnumber(res, "collname")));
	}

	PQclear(res);
}

PQLCompositeType *
getCompositeTypes(PGconn *c, int *n)
{
	PQLCompositeType	*t;
	PGresult			*res;
	int					i;

	logNoise("composite type: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				"SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'c' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");

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
		t = (PQLCompositeType *) malloc(*n * sizeof(PQLCompositeType));
	else
		t = NULL;

	logDebug("number of composite types in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		t[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "typname")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
			t[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		t[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "typowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "typacl")))
			t[i].acl = NULL;
		else
			t[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "typacl")));

		/* fill composite type attributes */
		getCompositeTypeAttributes(c, &t[i]);

		logDebug("composite type \"%s\".\"%s\"", t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

static void
getEnumTypeLabels(PGconn *c, PQLEnumType *t)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i;
	int			r;

	do {
		query = (char *) malloc(nquery * sizeof(char));

		/* enumsortorder is new in 9.1 */
		if (PQserverVersion(c) >= 90100)
			r = snprintf(query, nquery,
					"SELECT enumlabel FROM pg_enum WHERE enumtypid = %u ORDER BY enumsortorder", t->obj.oid);
		else
			r = snprintf(query, nquery,
					"SELECT enumlabel FROM pg_enum WHERE enumtypid = %u ORDER BY oid", t->obj.oid);

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

	t->nlabels = PQntuples(res);
	if (t->nlabels > 0)
		t->labels = (char **) malloc(t->nlabels * sizeof(char *));
	else
		t->labels = NULL;

	logDebug("number of labels on enum type \"%s\".\"%s\": %d", t->obj.schemaname, t->obj.objectname, t->nlabels);

	for (i = 0; i < t->nlabels; i++)
		t->labels[i] = strdup(PQgetvalue(res, i, PQfnumber(res, "enumlabel")));

	PQclear(res);
}

PQLEnumType *
getEnumTypes(PGconn *c, int *n)
{
	PQLEnumType		*t;
	PGresult		*res;
	int				i;

	logNoise("enum type: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				"SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'e' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");

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
		t = (PQLEnumType *) malloc(*n * sizeof(PQLEnumType));
	else
		t = NULL;

	logDebug("number of enum types in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		t[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "typname")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
			t[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		t[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "typowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "typacl")))
			t[i].acl = NULL;
		else
			t[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "typacl")));

		/* fill enum type labels */
		getEnumTypeLabels(c, &t[i]);

		logDebug("enum type \"%s\".\"%s\"", t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

PQLRangeType *
getRangeTypes(PGconn *c, int *n)
{
	PQLRangeType	*t;
	PGresult		*res;
	int				i;

	logNoise("range type: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				"SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, format_type(rngsubtype, NULL) AS subtype, m.nspname AS opcnspname, o.opcname, o.opcdefault, x.nspname AS collschemaname, CASE WHEN rngcollation = t.typcollation THEN NULL ELSE rngcollation END AS collname, rngcanonical, rngsubdiff, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) INNER JOIN pg_range r ON (r.rngsubtype = t.oid) INNER JOIN pg_opclass o ON (r.rngsubopc = o.oid) INNER JOIN pg_namespace m ON (o.opcnamespace = m.oid) LEFT JOIN (pg_collation l INNER JOIN pg_namespace x ON (l.collnamespace = x.oid)) ON (r.rngcollation = l.oid) WHERE t.typtype = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");

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
		t = (PQLRangeType *) malloc(*n * sizeof(PQLRangeType));
	else
		t = NULL;

	logDebug("number of range types in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		t[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "typname")));
		t[i].subtype = strdup(PQgetvalue(res, i, PQfnumber(res, "subtype")));
		t[i].opcschemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "opcnspname")));
		t[i].opcname = strdup(PQgetvalue(res, i, PQfnumber(res, "opcname")));
		t[i].opcdefault = (PQgetvalue(res, i, PQfnumber(res, "opcdefault"))[0] == 't');
		if (PQgetisnull(res, i, PQfnumber(res, "collname")))
		{
			t[i].collschemaname = NULL;
			t[i].collname = NULL;
		}
		else
		{
			t[i].collschemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "collschemaname")));
			t[i].collname = strdup(PQgetvalue(res, i, PQfnumber(res, "collname")));
		}
		t[i].canonical = strdup(PQgetvalue(res, i, PQfnumber(res, "rngcanonical")));
		t[i].diff = strdup(PQgetvalue(res, i, PQfnumber(res, "rngsubdiff")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
			t[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		t[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "typowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "typacl")))
			t[i].acl = NULL;
		else
			t[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "typacl")));

		logDebug("range type \"%s\".\"%s\"", t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

void
freeBaseTypes(PQLBaseType *t, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(t[i].obj.schemaname);
			free(t[i].obj.objectname);
			free(t[i].input);
			free(t[i].output);
			free(t[i].receive);
			free(t[i].send);
			free(t[i].modin);
			free(t[i].modout);
			free(t[i].analyze);
			if (t[i].typdefault)
				free(t[i].typdefault);
			free(t[i].category);
			free(t[i].delimiter);
			free(t[i].align);
			free(t[i].storage);
			free(t[i].owner);
			if (t[i].acl)
				free(t[i].acl);
			if (t[i].comment)
				free(t[i].comment);
		}

		free(t);
	}
}

void
freeCompositeTypes(PQLCompositeType *t, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(t[i].obj.schemaname);
			free(t[i].obj.objectname);
			free(t[i].owner);
			if (t[i].acl)
				free(t[i].acl);
			if (t[i].comment)
				free(t[i].comment);

			for (j = 0; j < t[i].nattributes; j++)
			{
				free(t[i].attributes[j].attname);
				free(t[i].attributes[j].typname);
				if (t[i].attributes[j].collschemaname)
					free(t[i].attributes[j].collschemaname);
				if (t[i].attributes[j].collname)
					free(t[i].attributes[j].collname);
			}
		}

		free(t);
	}
}

void
freeEnumTypes(PQLEnumType *t, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(t[i].obj.schemaname);
			free(t[i].obj.objectname);
			free(t[i].owner);
			if (t[i].acl)
				free(t[i].acl);
			if (t[i].comment)
				free(t[i].comment);

			for (j = 0; j < t[i].nlabels; j++)
				free(t[i].labels[j]);

			free(t[i].labels);
		}

		free(t);
	}
}

void
freeRangeTypes(PQLRangeType *t, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(t[i].obj.schemaname);
			free(t[i].obj.objectname);
			free(t[i].subtype);
			free(t[i].opcschemaname);
			free(t[i].opcname);
			if (t[i].collschemaname)
				free(t[i].collschemaname);
			if (t[i].collname)
				free(t[i].collname);
			free(t[i].canonical);
			free(t[i].diff);
			free(t[i].owner);
			if (t[i].acl)
				free(t[i].acl);
			if (t[i].comment)
				free(t[i].comment);
		}

		free(t);
	}
}

void
dumpCreateBaseType(FILE *output, PQLBaseType t)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s (",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname));

	fprintf(output, "\n\tINPUT = %s", t.input);
	fprintf(output, ",\n\tOUTPUT = %s", t.output);

	if (t.receive != NULL)
		fprintf(output, ",\n\tRECEIVE = %s", t.receive);
	if (t.send != NULL)
		fprintf(output, ",\n\tSEND = %s", t.send);
	if (t.modin != NULL)
		fprintf(output, ",\n\tTYPMOD_IN = %s", t.modin);
	if (t.modout != NULL)
		fprintf(output, ",\n\tTYPMOD_OUT = %s", t.modout);
	if (t.analyze != NULL)
		fprintf(output, ",\n\tANALYZE = %s", t.analyze);
	/* XXX ignore null-terminated string (-2)? */
	if (t.length < 0)
		fprintf(output, ",\n\tINTERNALLENGTH = VARIABLE");
	else
		fprintf(output, ",\n\tINTERNALLENGTH = %d", t.length);
	if (t.byvalue)
		fprintf(output, ",\n\tPASSEDBYVALUE");

	if (strcmp(t.align, "c") == 0)
		fprintf(output, ",\n\tALIGNMENT = char");
	else if (strcmp(t.align, "s") == 0)
		fprintf(output, ",\n\tALIGNMENT = int2");
	else if (strcmp(t.align, "i") == 0)
		fprintf(output, ",\n\tALIGNMENT = int4");
	else if (strcmp(t.align, "d") == 0)
		fprintf(output, ",\n\tALIGNMENT = double");

	if (strcmp(t.storage, "p") == 0)
		fprintf(output, ",\n\tSTORAGE = plain");
	else if (strcmp(t.storage, "e") == 0)
		fprintf(output, ",\n\tSTORAGE = external");
	else if (strcmp(t.storage, "m") == 0)
		fprintf(output, ",\n\tSTORAGE = main");
	else if (strcmp(t.storage, "x") == 0)
		fprintf(output, ",\n\tSTORAGE = extended");

	if (strcmp(t.category, "U") != 0)
		fprintf(output, ",\n\tCATEGORY = %s", t.category);

	if (t.preferred)
		fprintf(output, ",\n\tPREFERRED = true");

	/* always quote default value */
	if (t.typdefault != NULL)
		fprintf(output, ",\n\tDEFAULT = '%s'", t.typdefault);

	/* TODO implement ELEMENT */

	if (t.delimiter != NULL && strcmp(t.delimiter, ",") != 0)
		fprintf(output, ",\n\tDELIMITER = '%s'", t.delimiter);

	if (t.collatable)
		fprintf(output, ",\n\tCOLLATABLE = true");

	fprintf(output, "\n);");

	/* comment */
	if (options.comment && t.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.owner);
	}

	/* privileges */
	/* XXX second t.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, t.obj, t.obj, NULL, t.acl, NULL);
}

void
dumpCreateCompositeType(FILE *output, PQLCompositeType t)
{
	int		i;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s AS (",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname));

	for (i = 0; i < t.nattributes; i++)
	{
		if (i > 0)
			fprintf(output, ",");
		fprintf(output, "\n\t%s %s", t.attributes[i].attname, t.attributes[i].typname);

		if (t.attributes[i].collname != NULL)
			fprintf(output, " COLLATE %s.%s",
					formatObjectIdentifier(t.attributes[i].collschemaname),
					formatObjectIdentifier(t.attributes[i].collname));
	}

	fprintf(output, "\n);");

	/* comment */
	if (options.comment && t.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.owner);
	}

	/* privileges */
	/* XXX second t.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, t.obj, t.obj, NULL, t.acl, NULL);
}

void
dumpCreateEnumType(FILE *output, PQLEnumType t)
{
	int		i;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s AS ENUM (",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname));

	for (i = 0; i < t.nlabels; i++)
	{
		if (i > 0)
			fprintf(output, ",");
		fprintf(output, "\n\t'%s'", t.labels[i]);
	}

	fprintf(output, "\n);\n\n");

	/* comment */
	if (options.comment && t.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.owner);
	}

	/* privileges */
	/* XXX second t.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, t.obj, t.obj, NULL, t.acl, NULL);
}

void
dumpCreateRangeType(FILE *output, PQLRangeType t)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s AS RANGE (",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname));

	fprintf(output, "\n\tSUBTYPE = %s", t.subtype);

	/* print only if it isn't the default operator class */
	if (!t.opcdefault)
		fprintf(output, ",\n\tSUBTYPE_OPCLASS = %s.%s", formatObjectIdentifier(t.opcschemaname), formatObjectIdentifier(t.opcname));

	if (t.collname != NULL)
		fprintf(output, ",\n\tCOLLATION = %s.%s", formatObjectIdentifier(t.collschemaname), formatObjectIdentifier(t.collname));

	if (strcmp(t.canonical, "-") != 0)
		fprintf(output, ",\n\tCANONICAL = %s", t.canonical);

	if (strcmp(t.diff, "-") != 0)
		fprintf(output, ",\n\tSUBTYPE_DIFF = %s", t.diff);

	fprintf(output, "\n);");

	/* comment */
	if (options.comment && t.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.owner);
	}

	/* privileges */
	/* XXX second t.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, t.obj, t.obj, NULL, t.acl, NULL);
}

void
dumpDropBaseType(FILE *output, PQLBaseType t)
{
		fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", formatObjectIdentifier(t.obj.schemaname), formatObjectIdentifier(t.obj.objectname));
}

void
dumpDropCompositeType(FILE *output, PQLCompositeType t)
{
		fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", formatObjectIdentifier(t.obj.schemaname), formatObjectIdentifier(t.obj.objectname));
}

void
dumpDropEnumType(FILE *output, PQLEnumType t)
{
		fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", formatObjectIdentifier(t.obj.schemaname), formatObjectIdentifier(t.obj.objectname));
}

void
dumpDropRangeType(FILE *output, PQLRangeType t)
{
		fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", formatObjectIdentifier(t.obj.schemaname), formatObjectIdentifier(t.obj.objectname));
}

void
dumpAlterBaseType(FILE *output, PQLBaseType a, PQLBaseType b)
{
	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;",
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, a.obj, b.obj, a.acl, b.acl, NULL);
	}
}

void
dumpAlterCompositeType(FILE *output, PQLCompositeType a, PQLCompositeType b)
{
	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;",
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, a.obj, b.obj, a.acl, b.acl, NULL);
	}
}

void
dumpAlterEnumType(FILE *output, PQLEnumType a, PQLEnumType b)
{
	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;",
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, a.obj, b.obj, a.acl, b.acl, NULL);
	}
}

void
dumpAlterRangeType(FILE *output, PQLRangeType a, PQLRangeType b)
{
	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;",
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, a.obj, b.obj, a.acl, b.acl, NULL);
	}
}
