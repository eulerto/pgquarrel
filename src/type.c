/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * type.c
 *     Generate TYPE commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "type.h"


static void getCompositeTypeAttributes(PGconn *c, PQLCompositeType *t);
static void getEnumTypeLabels(PGconn *c, PQLEnumType *t);


PQLBaseType *
getBaseTypes(PGconn *c, int *n)
{
	PQLBaseType		*t;
	PGresult		*res;
	int				i;

	logNoise("base type: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90200)		/* support for privileges on data types */
	{
		res = PQexec(c,
					 "SELECT t.oid, n.nspname, t.typname, typlen AS length, typinput AS input, typoutput AS output, typreceive AS receive, typsend AS send, typmodin AS modin, typmodout AS modout, typanalyze AS analyze, (typcollation <> 0) as collatable, typdefault, typcategory AS category, typispreferred AS preferred, typdelim AS delimiter, typalign AS align, typstorage AS storage, typbyval AS byvalue, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'b' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		/* typcollation is new in 9.1 */
		res = PQexec(c,
					 "SELECT t.oid, n.nspname, t.typname, typlen AS length, typinput AS input, typoutput AS output, typreceive AS receive, typsend AS send, typmodin AS modin, typmodout AS modout, typanalyze AS analyze, (typcollation <> 0) as collatable, typdefault, typcategory AS category, typispreferred AS preferred, typdelim AS delimiter, typalign AS align, typstorage AS storage, typbyval AS byvalue, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'b' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else
	{
		res = PQexec(c,
					 "SELECT t.oid, n.nspname, t.typname, typlen AS length, typinput AS input, typoutput AS output, typreceive AS receive, typsend AS send, typmodin AS modin, typmodout AS modout, typanalyze AS analyze, false AS collatable, typdefault, typcategory AS category, typispreferred AS preferred, typdelim AS delimiter, typalign AS align, typstorage AS storage, typbyval AS byvalue, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'b' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY n.nspname, t.typname");
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

		/*
		 * Security labels are not assigned here (see getBaseTypeSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeBaseTypes.
		 */
		t[i].nseclabels = 0;
		t[i].seclabels = NULL;

		logDebug("base type \"%s\".\"%s\"", t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

void
getBaseTypeSecurityLabels(PGconn *c, PQLBaseType *t)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	/*
	 * Don't bother to check the kind of type because can't be duplicated oids
	 * in the same catalog.
	 */
	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_type' AND s.objoid = %u ORDER BY provider", t->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	t->nseclabels = PQntuples(res);
	if (t->nseclabels > 0)
		t->seclabels = (PQLSecLabel *) malloc(t->nseclabels * sizeof(PQLSecLabel));
	else
		t->seclabels = NULL;

	logDebug("number of security labels in base type \"%s\".\"%s\": %d", t->obj.schemaname, t->obj.objectname, t->nseclabels);

	for (i = 0; i < t->nseclabels; i++)
	{
		t->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		t->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

/* TODO composite type column comments */
static void
getCompositeTypeAttributes(PGconn *c, PQLCompositeType *t)
{
	char		*query = NULL;
	int			nquery = 0;
	PGresult	*res;
	int			i;

	/* typcollation is new in 9.1 */
	if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		/* determine how many characters will be written by snprintf */
		nquery = snprintf(query, nquery,
					 "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS attdefinition, p.nspname AS collschemaname, CASE WHEN a.attcollation <> u.typcollation THEN l.collname ELSE NULL END AS collname FROM pg_type t INNER JOIN pg_attribute a ON (a.attrelid = t.typrelid) LEFT JOIN pg_type u ON (u.oid = a.atttypid) LEFT JOIN (pg_collation l LEFT JOIN pg_namespace p ON (l.collnamespace = p.oid)) ON (a.attcollation = l.oid) WHERE t.oid = %u AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY a.attnum",
					 t->obj.oid);

		nquery++;
		query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
		snprintf(query, nquery,
					 "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS attdefinition, p.nspname AS collschemaname, CASE WHEN a.attcollation <> u.typcollation THEN l.collname ELSE NULL END AS collname FROM pg_type t INNER JOIN pg_attribute a ON (a.attrelid = t.typrelid) LEFT JOIN pg_type u ON (u.oid = a.atttypid) LEFT JOIN (pg_collation l LEFT JOIN pg_namespace p ON (l.collnamespace = p.oid)) ON (a.attcollation = l.oid) WHERE t.oid = %u AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY a.attnum",
					 t->obj.oid);

		logNoise("composite type: query size: %d ; query: %s", nquery, query);
	}
	else
	{
		/* determine how many characters will be written by snprintf */
		nquery = snprintf(query, nquery,
					 "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS attdefinition, NULL AS collschemaname, NULL AS collname FROM pg_type t INNER JOIN pg_attribute a ON (a.attrelid = t.typrelid) WHERE t.oid = %u ORDER BY a.attnum",
					 t->obj.oid);

		nquery++;
		query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
		snprintf(query, nquery,
					 "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS attdefinition, NULL AS collschemaname, NULL AS collname FROM pg_type t INNER JOIN pg_attribute a ON (a.attrelid = t.typrelid) WHERE t.oid = %u ORDER BY a.attnum",
					 t->obj.oid);

		logNoise("composite type: query size: %d ; query: %s", nquery, query);
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

	t->nattributes = PQntuples(res);
	if (t->nattributes > 0)
		t->attributes = (PQLAttrCompositeType *) malloc(t->nattributes * sizeof(
							PQLAttrCompositeType));
	else
		t->attributes = NULL;

	logDebug("number of attributes on composite type \"%s\".\"%s\": %d",
			 t->obj.schemaname, t->obj.objectname, t->nattributes);

	for (i = 0; i < t->nattributes; i++)
	{
		t->attributes[i].attname = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "attname")));
		t->attributes[i].typname = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "attdefinition")));
		/* collation can be NULL in 9.0 or earlier */
		if (PQgetisnull(res, i, PQfnumber(res, "collschemaname")))
			t->attributes[i].collschemaname = NULL;
		else
			t->attributes[i].collschemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
											  "collschemaname")));
		if (PQgetisnull(res, i, PQfnumber(res, "collname")))
			t->attributes[i].collname = NULL;
		else
			t->attributes[i].collname = strdup(PQgetvalue(res, i, PQfnumber(res,
											   "collname")));
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

	if (PQserverVersion(c) >= 90200)	/* support for privileges on data types */
	{
		res = PQexec(c,
				 "SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'c' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		res = PQexec(c,
				 "SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'c' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else
	{
		res = PQexec(c,
				 "SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'c' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY n.nspname, t.typname");
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

		/*
		 * Security labels are not assigned here (see getCompositeTypeSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeCompositeTypes.
		 */
		t[i].nseclabels = 0;
		t[i].seclabels = NULL;

		/* fill composite type attributes */
		getCompositeTypeAttributes(c, &t[i]);

		logDebug("composite type \"%s\".\"%s\"", t[i].obj.schemaname,
				 t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

void
getCompositeTypeSecurityLabels(PGconn *c, PQLCompositeType *t)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	/*
	 * Don't bother to check the kind of type because can't be duplicated oids
	 * in the same catalog.
	 */
	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_type' AND s.objoid = %u ORDER BY provider", t->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	t->nseclabels = PQntuples(res);
	if (t->nseclabels > 0)
		t->seclabels = (PQLSecLabel *) malloc(t->nseclabels * sizeof(PQLSecLabel));
	else
		t->seclabels = NULL;

	logDebug("number of security labels in composite type \"%s\".\"%s\": %d", t->obj.schemaname, t->obj.objectname, t->nseclabels);

	for (i = 0; i < t->nseclabels; i++)
	{
		t->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		t->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

static void
getEnumTypeLabels(PGconn *c, PQLEnumType *t)
{
	char		*query = NULL;
	int			nquery = 0;
	PGresult	*res;
	int			i;

	/* enumsortorder is new in 9.1 */
	if (PQserverVersion(c) >= 90100)
	{
		/* determine how many characters will be written by snprintf */
		nquery = snprintf(query, nquery,
					 "SELECT enumlabel FROM pg_enum WHERE enumtypid = %u ORDER BY enumsortorder",
					 t->obj.oid);

		nquery++;
		query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
		snprintf(query, nquery,
					 "SELECT enumlabel FROM pg_enum WHERE enumtypid = %u ORDER BY enumsortorder",
					 t->obj.oid);

		logNoise("enum type: query size: %d ; query: %s", nquery, query);
	}
	else
	{
		/* determine how many characters will be written by snprintf */
		nquery = snprintf(query, nquery,
					 "SELECT enumlabel FROM pg_enum WHERE enumtypid = %u ORDER BY oid", t->obj.oid);

		nquery++;
		query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
		snprintf(query, nquery,
					 "SELECT enumlabel FROM pg_enum WHERE enumtypid = %u ORDER BY oid", t->obj.oid);

		logNoise("enum type: query size: %d ; query: %s", nquery, query);
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

	t->nlabels = PQntuples(res);
	if (t->nlabels > 0)
		t->labels = (char **) malloc(t->nlabels * sizeof(char *));
	else
		t->labels = NULL;

	logDebug("number of labels on enum type \"%s\".\"%s\": %d", t->obj.schemaname,
			 t->obj.objectname, t->nlabels);

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

	if (PQserverVersion(c) >= 90200)		/* support for privileges on data types */
	{
		res = PQexec(c,
				 "SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'e' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		res = PQexec(c,
				 "SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'e' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, t.typname");
	}
	else
	{
		res = PQexec(c,
				 "SELECT t.oid, n.nspname, t.typname, obj_description(t.oid, 'pg_type') AS description, pg_get_userbyid(t.typowner) AS typowner, NULL AS typacl FROM pg_type t INNER JOIN pg_namespace n ON (t.typnamespace = n.oid) WHERE t.typtype = 'e' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY n.nspname, t.typname");
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

		/*
		 * Security labels are not assigned here (see getEnumTypeSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeEnumTypes.
		 */
		t[i].nseclabels = 0;
		t[i].seclabels = NULL;

		/* fill enum type labels */
		getEnumTypeLabels(c, &t[i]);

		logDebug("enum type \"%s\".\"%s\"", t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

void
getEnumTypeSecurityLabels(PGconn *c, PQLEnumType *t)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	/*
	 * Don't bother to check the kind of type because can't be duplicated oids
	 * in the same catalog.
	 */
	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_type' AND s.objoid = %u ORDER BY provider", t->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	t->nseclabels = PQntuples(res);
	if (t->nseclabels > 0)
		t->seclabels = (PQLSecLabel *) malloc(t->nseclabels * sizeof(PQLSecLabel));
	else
		t->seclabels = NULL;

	logDebug("number of security labels in enum type \"%s\".\"%s\": %d", t->obj.schemaname, t->obj.objectname, t->nseclabels);

	for (i = 0; i < t->nseclabels; i++)
	{
		t->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		t->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

PQLRangeType *
getRangeTypes(PGconn *c, int *n)
{
	PQLRangeType	*t;
	PGresult		*res;
	int				i;

	logNoise("range type: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 90200)
	{
		logWarning("ignoring range types because server does not support it");
		return NULL;
	}

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
			t[i].collschemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
													"collschemaname")));
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

		/*
		 * Security labels are not assigned here (see getRangeTypeSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeRangeTypes.
		 */
		t[i].nseclabels = 0;
		t[i].seclabels = NULL;

		logDebug("range type \"%s\".\"%s\"", t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

void
getRangeTypeSecurityLabels(PGconn *c, PQLRangeType *t)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	/*
	 * Don't bother to check the kind of type because can't be duplicated oids
	 * in the same catalog.
	 */
	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_type' AND s.objoid = %u ORDER BY provider", t->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	t->nseclabels = PQntuples(res);
	if (t->nseclabels > 0)
		t->seclabels = (PQLSecLabel *) malloc(t->nseclabels * sizeof(PQLSecLabel));
	else
		t->seclabels = NULL;

	logDebug("number of security labels in range type \"%s\".\"%s\": %d", t->obj.schemaname, t->obj.objectname, t->nseclabels);

	for (i = 0; i < t->nseclabels; i++)
	{
		t->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		t->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

void
freeBaseTypes(PQLBaseType *t, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

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

			/* security labels */
			for (j = 0; j < t[i].nseclabels; j++)
			{
				free(t[i].seclabels[j].provider);
				free(t[i].seclabels[j].label);
			}

			if (t[i].seclabels)
				free(t[i].seclabels);
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

			/* security labels */
			for (j = 0; j < t[i].nseclabels; j++)
			{
				free(t[i].seclabels[j].provider);
				free(t[i].seclabels[j].label);
			}

			if (t[i].seclabels)
				free(t[i].seclabels);

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

			/* security labels */
			for (j = 0; j < t[i].nseclabels; j++)
			{
				free(t[i].seclabels[j].provider);
				free(t[i].seclabels[j].label);
			}

			if (t[i].seclabels)
				free(t[i].seclabels);

			/* labels */
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
			int	j;

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

			/* security labels */
			for (j = 0; j < t[i].nseclabels; j++)
			{
				free(t[i].seclabels[j].provider);
				free(t[i].seclabels[j].label);
			}

			if (t[i].seclabels)
				free(t[i].seclabels);
		}

		free(t);
	}
}

void
dumpCreateBaseType(FILE *output, PQLBaseType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s (", schema, typname);

	fprintf(output, "\n\tINPUT = %s", t->input);
	fprintf(output, ",\n\tOUTPUT = %s", t->output);

	if (t->receive != NULL)
		fprintf(output, ",\n\tRECEIVE = %s", t->receive);
	if (t->send != NULL)
		fprintf(output, ",\n\tSEND = %s", t->send);
	if (t->modin != NULL)
		fprintf(output, ",\n\tTYPMOD_IN = %s", t->modin);
	if (t->modout != NULL)
		fprintf(output, ",\n\tTYPMOD_OUT = %s", t->modout);
	if (t->analyze != NULL)
		fprintf(output, ",\n\tANALYZE = %s", t->analyze);
	/* XXX ignore null-terminated string (-2)? */
	if (t->length < 0)
		fprintf(output, ",\n\tINTERNALLENGTH = VARIABLE");
	else
		fprintf(output, ",\n\tINTERNALLENGTH = %d", t->length);
	if (t->byvalue)
		fprintf(output, ",\n\tPASSEDBYVALUE");

	if (strcmp(t->align, "c") == 0)
		fprintf(output, ",\n\tALIGNMENT = char");
	else if (strcmp(t->align, "s") == 0)
		fprintf(output, ",\n\tALIGNMENT = int2");
	else if (strcmp(t->align, "i") == 0)
		fprintf(output, ",\n\tALIGNMENT = int4");
	else if (strcmp(t->align, "d") == 0)
		fprintf(output, ",\n\tALIGNMENT = double");

	if (strcmp(t->storage, "p") == 0)
		fprintf(output, ",\n\tSTORAGE = plain");
	else if (strcmp(t->storage, "e") == 0)
		fprintf(output, ",\n\tSTORAGE = external");
	else if (strcmp(t->storage, "m") == 0)
		fprintf(output, ",\n\tSTORAGE = main");
	else if (strcmp(t->storage, "x") == 0)
		fprintf(output, ",\n\tSTORAGE = extended");

	if (strcmp(t->category, "U") != 0)
		fprintf(output, ",\n\tCATEGORY = %s", t->category);

	if (t->preferred)
		fprintf(output, ",\n\tPREFERRED = true");

	/* always quote default value */
	if (t->typdefault != NULL)
		fprintf(output, ",\n\tDEFAULT = '%s'", t->typdefault);

	/* TODO implement ELEMENT */

	if (t->delimiter != NULL && strcmp(t->delimiter, ",") != 0)
		fprintf(output, ",\n\tDELIMITER = '%s'", t->delimiter);

	if (t->collatable)
		fprintf(output, ",\n\tCOLLATABLE = true");

	fprintf(output, "\n);");

	/* comment */
	if (options.comment && t->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema, typname, t->comment);
	}

	/* security labels */
	if (options.securitylabels && t->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < t->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
					t->seclabels[i].provider,
					schema,
					typname,
					t->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema, typname, t->owner);
	}

	/* privileges */
	/* XXX second t->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, &t->obj, &t->obj, NULL, t->acl, NULL, NULL);

	free(schema);
	free(typname);
}

void
dumpCreateCompositeType(FILE *output, PQLCompositeType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	int		i;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s AS (", schema, typname);

	for (i = 0; i < t->nattributes; i++)
	{
		if (i > 0)
			fprintf(output, ",");
		fprintf(output, "\n\t%s %s", t->attributes[i].attname, t->attributes[i].typname);

		if (t->attributes[i].collname != NULL)
		{
			char	*attschema = formatObjectIdentifier(t->attributes[i].collschemaname);
			char	*attcollation = formatObjectIdentifier(t->attributes[i].collname);

			fprintf(output, " COLLATE %s.%s", attschema, attcollation);

			free(attschema);
			free(attcollation);
		}
	}

	fprintf(output, "\n);");

	/* comment */
	if (options.comment && t->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema, typname, t->comment);
	}

	/* security labels */
	if (options.securitylabels && t->nseclabels > 0)
	{
		for (i = 0; i < t->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
					t->seclabels[i].provider,
					schema,
					typname,
					t->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema, typname, t->owner);
	}

	/* privileges */
	/* XXX second t->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, &t->obj, &t->obj, NULL, t->acl, NULL, NULL);

	free(schema);
	free(typname);
}

void
dumpCreateEnumType(FILE *output, PQLEnumType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	int		i;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s AS ENUM (", schema, typname);

	for (i = 0; i < t->nlabels; i++)
	{
		if (i > 0)
			fprintf(output, ",");
		fprintf(output, "\n\t'%s'", t->labels[i]);
	}

	fprintf(output, "\n);\n\n");

	/* comment */
	if (options.comment && t->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema, typname, t->comment);
	}

	/* security labels */
	if (options.securitylabels && t->nseclabels > 0)
	{
		for (i = 0; i < t->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
					t->seclabels[i].provider,
					schema,
					typname,
					t->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema, typname, t->owner);
	}

	/* privileges */
	/* XXX second t->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, &t->obj, &t->obj, NULL, t->acl, NULL, NULL);

	free(schema);
	free(typname);
}

void
dumpCreateRangeType(FILE *output, PQLRangeType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TYPE %s.%s AS RANGE (", schema, typname);

	fprintf(output, "\n\tSUBTYPE = %s", t->subtype);

	/* print only if it isn't the default operator class */
	if (!t->opcdefault)
	{
		char	*opcschema = formatObjectIdentifier(t->opcschemaname);
		char	*opcname = formatObjectIdentifier(t->opcname);

		fprintf(output, ",\n\tSUBTYPE_OPCLASS = %s.%s", opcschema, opcname);

		free(opcschema);
		free(opcname);
	}

	if (t->collname != NULL)
	{
		char	*collschema = formatObjectIdentifier(t->collschemaname);
		char	*collname = formatObjectIdentifier(t->collname);

		fprintf(output, ",\n\tCOLLATION = %s.%s", t->collschemaname, t->collname);

		free(collschema);
		free(collname);
	}

	if (strcmp(t->canonical, "-") != 0)
		fprintf(output, ",\n\tCANONICAL = %s", t->canonical);

	if (strcmp(t->diff, "-") != 0)
		fprintf(output, ",\n\tSUBTYPE_DIFF = %s", t->diff);

	fprintf(output, "\n);");

	/* comment */
	if (options.comment && t->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema, typname, t->comment);
	}

	/* security labels */
	if (options.securitylabels && t->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < t->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
					t->seclabels[i].provider,
					schema,
					typname,
					t->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema, typname, t->owner);
	}

	/* privileges */
	/* XXX second t->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TYPE, &t->obj, &t->obj, NULL, t->acl, NULL, NULL);

	free(schema);
	free(typname);
}

void
dumpDropBaseType(FILE *output, PQLBaseType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", schema, typname);

	free(schema);
	free(typname);
}

void
dumpDropCompositeType(FILE *output, PQLCompositeType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", schema, typname);

	free(schema);
	free(typname);
}

void
dumpDropEnumType(FILE *output, PQLEnumType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", schema, typname);

	free(schema);
	free(typname);
}

void
dumpDropRangeType(FILE *output, PQLRangeType *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*typname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TYPE %s.%s;", schema, typname);

	free(schema);
	free(typname);
}

void
dumpAlterBaseType(FILE *output, PQLBaseType *a, PQLBaseType *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*typname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*typname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema2, typname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;", schema2, typname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
						b->seclabels[i].provider,
						schema2,
						typname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						typname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
								b->seclabels[j].provider,
								schema2,
								typname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema2, typname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, &a->obj, &b->obj, a->acl, b->acl, NULL, NULL);
	}

	free(schema1);
	free(typname1);
	free(schema2);
	free(typname2);
}

void
dumpAlterCompositeType(FILE *output, PQLCompositeType *a, PQLCompositeType *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*typname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*typname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema2, typname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;", schema2, typname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
						b->seclabels[i].provider,
						schema2,
						typname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						typname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
								b->seclabels[j].provider,
								schema2,
								typname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema2, typname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, &a->obj, &b->obj, a->acl, b->acl, NULL, NULL);
	}

	free(schema1);
	free(typname1);
	free(schema2);
	free(typname2);
}

void
dumpAlterEnumType(FILE *output, PQLEnumType *a, PQLEnumType *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*typname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*typname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema2, typname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;", schema2, typname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
						b->seclabels[i].provider,
						schema2,
						typname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						typname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
								b->seclabels[j].provider,
								schema2,
								typname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema2, typname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, &a->obj, &b->obj, a->acl, b->acl, NULL, NULL);
	}

	free(schema1);
	free(typname1);
	free(schema2);
	free(typname2);
}

void
dumpAlterRangeType(FILE *output, PQLRangeType *a, PQLRangeType *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*typname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*typname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS '%s';", schema2, typname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TYPE %s.%s IS NULL;", schema2, typname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
						b->seclabels[i].provider,
						schema2,
						typname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
						a->seclabels[i].provider,
						schema1,
						typname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
								b->seclabels[j].provider,
								schema2,
								typname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS NULL;",
							a->seclabels[i].provider,
							schema1,
							typname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TYPE %s.%s IS '%s';",
							b->seclabels[j].provider,
							schema2,
							typname2,
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
			fprintf(output, "ALTER TYPE %s.%s OWNER TO %s;", schema2, typname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TYPE, &a->obj, &b->obj, a->acl, b->acl, NULL, NULL);
	}

	free(schema1);
	free(typname1);
	free(schema2);
	free(typname2);
}
