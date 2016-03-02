#include "table.h"

/*
 * CREATE TABLE
 * DROP TABLE
 * ALTER TABLE
 * COMMENT ON TABLE
 * COMMENT ON COLUMN
 * COMMENT ON CONSTRAINT
 *
 * TODO
 *
 * CREATE TABLE ... INHERITS
 * CREATE TABLE ... OF type_name
 * CREATE TABLE ... TABLESPACE
 * CREATE TABLE ... EXCLUDE
 *
 * ALTER TABLE ... RENAME COLUMN ... TO
 * ALTER TABLE ... RENAME CONSTRAINT ... TO
 * ALTER TABLE ... RENAME TO
 * ALTER TABLE ... SET SCHEMA TO
 * ALTER TABLE ... {ALTER VALIDATE | DROP } CONSTRAINT
 * ALTER TABLE ... { ENABLE | DISABLE } TRIGGER
 * ALTER TABLE ... { ENABLE | DISABLE } RULE
 * ALTER TABLE ... ENABLE REPLICA { TRIGGER | RULE }
 * ALTER TABLE ... CLUSTER ON index_name
 * ALTER TABLE ... SET WITHOUT CLUSTER
 * ALTER TABLE ... INHERIT parent_table
 * ALTER TABLE ... NOINHERIT parent_table
 * ALTER TABLE ... OF type_name
 * ALTER TABLE ... SET TABLESPACE
 * ALTER TABLE ... REPLICA IDENTITY
 */

static void dumpAddColumn(FILE *output, PQLTable t, int i);
static void dumpRemoveColumn(FILE *output, PQLTable t, int i);
static void dumpAlterColumn(FILE *output, PQLTable a, int i, PQLTable b, int j);
static void dumpAlterColumnSetStatistics(FILE *output, PQLTable a, int i,
		bool force);
static void dumpAlterColumnSetStorage(FILE *output, PQLTable a, int i,
									  bool force);
static void dumpAlterColumnSetOptions(FILE *output, PQLTable a, int i,
									  PQLTable b, int j);

PQLTable *
getTables(PGconn *c, int *n)
{
	PQLTable	*t;
	PGresult	*res;
	int			i;

	logNoise("table: server version: %d", PQserverVersion(c));

	/* FIXME relpersistence (9.1)? */
	/*
	 * XXX Using 'v' (void) to represent unsupported replica identity
	 */
	if (PQserverVersion(c) >= 90400)
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, relreplident FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'r' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");
	else if (PQserverVersion(c) >= 90100)
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, 'v' AS relreplident FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'r' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");
	else
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, 'p' AS relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, 'v' AS relreplident FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'r' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");

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
		t = (PQLTable *) malloc(*n * sizeof(PQLTable));
	else
		t = NULL;

	logDebug("number of tables in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		t[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		if (PQgetisnull(res, i, PQfnumber(res, "tablespacename")))
			t[i].tbspcname = NULL;
		else
			t[i].tbspcname = strdup(PQgetvalue(res, i, PQfnumber(res, "tablespacename")));
		t[i].unlogged = (PQgetvalue(res, i, PQfnumber(res,
									"relpersistence"))[0] == 'u');

		/*
		 * These values are not assigned here (see getTableAttributes), but
		 * default values are essential to avoid having trouble in freeTables.
		 */
		t[i].nattributes = 0;
		t[i].attributes = NULL;
		t[i].ncheck = 0;
		t[i].check = NULL;
		t[i].nfk = 0;
		t[i].fk = NULL;
		t[i].pk.conname = NULL;
		t[i].pk.condef = NULL;
		t[i].pk.comment = NULL;
		t[i].seqownedby = NULL;
		t[i].attownedby = NULL;
		t[i].nownedby = 0;

		if (PQgetisnull(res, i, PQfnumber(res, "reloptions")))
			t[i].reloptions = NULL;
		else
			t[i].reloptions = strdup(PQgetvalue(res, i, PQfnumber(res, "reloptions")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
			t[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		t[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "relowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "relacl")))
			t[i].acl = NULL;
		else
			t[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "relacl")));

		t[i].relreplident = *(PQgetvalue(res, i, PQfnumber(res, "relreplident")));
		/* assigned iif REPLICA IDENTITY USING INDEX; see getTableAttributes() */
		t[i].relreplidentidx = NULL;

		/*
		 * Security labels are not assigned here (see getTableSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeTables.
		 */
		t[i].nseclabels = 0;
		t[i].seclabels = NULL;

		logDebug("table %s.%s", formatObjectIdentifier(t[i].obj.schemaname),
				 formatObjectIdentifier(t[i].obj.objectname));
	}

	PQclear(res);

	return t;
}

void
getCheckConstraints(PGconn *c, PQLTable *t, int n)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i, j;
	int			r;

	for (i = 0; i < n; i++)
	{
		do
		{
			query = (char *) malloc(nquery * sizeof(char));

			/* FIXME conislocal (8.4)? convalidated (9.2)? */
			/* XXX contype = 'c' needed? */
			r = snprintf(query, nquery,
						 "SELECT conname, pg_get_constraintdef(c.oid) AS condef, d.description FROM pg_constraint c LEFT JOIN (pg_description d INNER JOIN pg_class x ON (x.oid = d.classoid AND x.relname = 'pg_constraint')) ON (d.objoid = c.oid) WHERE conrelid = %u AND contype = 'c' ORDER BY conname",
						 t[i].obj.oid);

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

		t[i].ncheck = PQntuples(res);
		if (t[i].ncheck > 0)
			t[i].check = (PQLConstraint *) malloc(t[i].ncheck * sizeof(PQLConstraint));
		else
			t[i].check = NULL;

		logDebug("number of check constraints in table %s.%s: %d",
				 formatObjectIdentifier(t[i].obj.schemaname),
				 formatObjectIdentifier(t[i].obj.objectname), t[i].ncheck);
		for (j = 0; j < t[i].ncheck; j++)
		{
			t[i].check[j].conname = strdup(PQgetvalue(res, j, PQfnumber(res, "conname")));
			t[i].check[j].condef = strdup(PQgetvalue(res, j, PQfnumber(res, "condef")));
			if (PQgetisnull(res, j, PQfnumber(res, "description")))
				t[i].check[j].comment = NULL;
			else
				t[i].check[j].comment = strdup(PQgetvalue(res, j, PQfnumber(res,
											   "description")));
		}

		PQclear(res);
	}
}

void
getFKConstraints(PGconn *c, PQLTable *t, int n)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i, j;
	int			r;

	for (i = 0; i < n; i++)
	{
		do
		{
			query = (char *) malloc(nquery * sizeof(char));

			r = snprintf(query, nquery,
						 "SELECT conname, pg_get_constraintdef(c.oid) AS condef, d.description FROM pg_constraint c LEFT JOIN (pg_description d INNER JOIN pg_class x ON (x.oid = d.classoid AND x.relname = 'pg_constraint')) ON (d.objoid = c.oid) WHERE conrelid = %u AND contype = 'f' ORDER BY conname",
						 t[i].obj.oid);

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

		t[i].nfk = PQntuples(res);
		if (t[i].nfk > 0)
			t[i].fk = (PQLConstraint *) malloc(t[i].nfk * sizeof(PQLConstraint));
		else
			t[i].fk = NULL;

		logDebug("number of FK constraints in table %s.%s: %d",
				 formatObjectIdentifier(t[i].obj.schemaname),
				 formatObjectIdentifier(t[i].obj.objectname), t[i].nfk);
		for (j = 0; j < t[i].nfk; j++)
		{
			t[i].fk[j].conname = strdup(PQgetvalue(res, j, PQfnumber(res, "conname")));
			t[i].fk[j].condef = strdup(PQgetvalue(res, j, PQfnumber(res, "condef")));
			if (PQgetisnull(res, j, PQfnumber(res, "description")))
				t[i].fk[j].comment = NULL;
			else
				t[i].fk[j].comment = strdup(PQgetvalue(res, j, PQfnumber(res, "description")));
		}

		PQclear(res);
	}
}

void
getPKConstraints(PGconn *c, PQLTable *t, int n)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i;
	int			r;

	for (i = 0; i < n; i++)
	{
		do
		{
			query = (char *) malloc(nquery * sizeof(char));

			/* XXX only 9.0+ */
			r = snprintf(query, nquery,
						 "SELECT conname, pg_get_constraintdef(c.oid) AS condef, d.description FROM pg_constraint c LEFT JOIN (pg_description d INNER JOIN pg_class x ON (x.oid = d.classoid AND x.relname = 'pg_constraint')) ON (d.objoid = c.oid) WHERE conrelid = %u AND contype = 'p' ORDER BY conname",
						 t[i].obj.oid);

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

		if (PQntuples(res) == 1)
		{
			t[i].pk.conname = strdup(PQgetvalue(res, 0, PQfnumber(res, "conname")));
			t[i].pk.condef = strdup(PQgetvalue(res, 0, PQfnumber(res, "condef")));
			if (PQgetisnull(res, 0, PQfnumber(res, "description")))
				t[i].pk.comment = NULL;
			else
				t[i].pk.comment = strdup(PQgetvalue(res, 0, PQfnumber(res, "description")));
		}
		/* XXX cannot happen */
		else if (PQntuples(res) > 1)
			logWarning("could not have more than one primary key");

		PQclear(res);
	}
}

void
getTableAttributes(PGconn *c, PQLTable *t)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i;
	int			r;

	do
	{
		query = (char *) malloc(nquery * sizeof(char));

		/* FIXME attcollation (9.1)? */
		r = snprintf(query, nquery,
					 "SELECT a.attnum, a.attname, a.attnotnull, pg_catalog.format_type(t.oid, a.atttypmod) as atttypname, pg_get_expr(d.adbin, a.attrelid) as attdefexpr, CASE WHEN a.attcollation <> t.typcollation THEN c.collname ELSE NULL END AS attcollation, s.description, a.attstattarget, a.attstorage, CASE WHEN t.typstorage <> a.attstorage THEN FALSE ELSE TRUE END AS defstorage, array_to_string(attoptions, ', ') AS attoptions FROM pg_attribute a LEFT JOIN pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum) LEFT JOIN pg_collation c ON (a.attcollation = c.oid) LEFT JOIN (pg_description s INNER JOIN pg_class x ON (x.oid = s.classoid AND x.relname = 'pg_attribute')) ON (s.objoid = c.oid) WHERE a.attrelid = %u AND a.attnum > 0 AND attisdropped IS FALSE ORDER BY a.attname",
					 t->obj.oid);

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

	t->nattributes = PQntuples(res);
	if (t->nattributes > 0)
		t->attributes = (PQLAttribute *) malloc(t->nattributes * sizeof(PQLAttribute));
	else
		t->attributes = NULL;

	logDebug("number of attributes in table %s.%s: %d",
			 formatObjectIdentifier(t->obj.schemaname),
			 formatObjectIdentifier(t->obj.objectname), t->nattributes);

	if (t->reloptions)
		logDebug("table %s.%s: reloptions: %s",
				 formatObjectIdentifier(t->obj.schemaname),
				 formatObjectIdentifier(t->obj.objectname),
				 t->reloptions);
	else
		logDebug("table %s.%s: no reloptions",
				 formatObjectIdentifier(t->obj.schemaname),
				 formatObjectIdentifier(t->obj.objectname));

	for (i = 0; i < t->nattributes; i++)
	{
		char	storage;

		t->attributes[i].attnum = strtoul(PQgetvalue(res, i, PQfnumber(res, "attnum")),
										  NULL, 10);
		t->attributes[i].attname = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "attname")));
		t->attributes[i].attnotnull = (PQgetvalue(res, i, PQfnumber(res,
									   "attnotnull"))[0] == 't');
		t->attributes[i].atttypname = strdup(PQgetvalue(res, i, PQfnumber(res,
											 "atttypname")));
		/* default expression */
		if (PQgetisnull(res, i, PQfnumber(res, "attdefexpr")))
			t->attributes[i].attdefexpr = NULL;
		else
			t->attributes[i].attdefexpr = strdup(PQgetvalue(res, i, PQfnumber(res,
												 "attdefexpr")));
		/* statistics target */
		t->attributes[i].attstattarget = atoi(PQgetvalue(res, i, PQfnumber(res,
											  "attstattarget")));

		/* storage */
		storage = PQgetvalue(res, i, PQfnumber(res, "attstorage"))[0];
		switch (storage)
		{
			case 'p':
				t->attributes[i].attstorage = strdup("PLAIN");
				break;
			case 'e':
				t->attributes[i].attstorage = strdup("EXTERNAL");
				break;
			case 'm':
				t->attributes[i].attstorage = strdup("MAIN");
				break;
			case 'x':
				t->attributes[i].attstorage = strdup("EXTENDED");
				break;
			default:
				t->attributes[i].attstorage = NULL;
				break;
		}
		t->attributes[i].defstorage = (PQgetvalue(res, i, PQfnumber(res,
									   "defstorage"))[0] == 't');

		/* collation */
		if (PQgetisnull(res, i, PQfnumber(res, "attcollation")))
			t->attributes[i].attcollation = NULL;
		else
			t->attributes[i].attcollation = strdup(PQgetvalue(res, i, PQfnumber(res,
												   "attcollation")));

		/* attribute options */
		if (PQgetisnull(res, i, PQfnumber(res, "attoptions")))
			t->attributes[i].attoptions = NULL;
		else
			t->attributes[i].attoptions = strdup(PQgetvalue(res, i, PQfnumber(res,
												 "attoptions")));

		/* comment */
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t->attributes[i].comment = NULL;
		else
			t->attributes[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res,
											  "description")));

		/*
		 * Security labels are not assigned here (see getTableSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeTables.
		 */
		t->attributes[i].nseclabels = 0;
		t->attributes[i].seclabels = NULL;

		if (t->attributes[i].attdefexpr != NULL)
			logDebug("table: %s.%s ; attribute %s; type: %s ; default: %s ; storage: %s",
					 formatObjectIdentifier(t->obj.schemaname),
					 formatObjectIdentifier(t->obj.objectname), t->attributes[i].attname,
					 t->attributes[i].atttypname, t->attributes[i].attdefexpr,
					 t->attributes[i].attstorage);
		else
			logDebug("table: %s.%s ; attribute %s; type: %s ; storage: %s",
					 formatObjectIdentifier(t->obj.schemaname),
					 formatObjectIdentifier(t->obj.objectname), t->attributes[i].attname,
					 t->attributes[i].atttypname,
					 t->attributes[i].attstorage);
	}

	PQclear(res);

	/* replica identity using index */
	if (t->relreplident == 'i')
	{
		do
		{
			query = (char *) malloc(nquery * sizeof(char));

			r = snprintf(query, nquery,
						 "SELECT c.relname AS  idxname FROM pg_index i INNER JOIN pg_class c ON (i.indexrelid = c.oid) WHERE indrelid = %u AND indisreplident",
						 t->obj.oid);

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

		i  = PQntuples(res);
		if (i == 1)
		{
			t->relreplidentidx = strdup(PQgetvalue(res, 0, PQfnumber(res,
												   "idxname")));
		}
		else
		{
			logWarning("table %s.%s should contain one replica identity index (returned %d)",
					   formatObjectIdentifier(t->obj.schemaname),
					   formatObjectIdentifier(t->obj.objectname), i);
		}

		PQclear(res);
	}
}

void
getTableSecurityLabels(PGconn *c, PQLTable *t)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PG_VERSION_NUM < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_class' AND s.objoid = %u ORDER BY provider", t->obj.oid);

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

	logDebug("number of security labels in table %s.%s: %d",
			 formatObjectIdentifier(t->obj.schemaname),
			 formatObjectIdentifier(t->obj.objectname), t->nseclabels);

	for (i = 0; i < t->nseclabels; i++)
	{
		t->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		t->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);

	/* attributes */
	for (i = 0; i < t->nattributes; i++)
	{
		int		j;

		snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_attribute' AND s.objoid = %u AND s.objsubid = %u ORDER BY provider", t->obj.oid, t->attributes[i].attnum);

		res = PQexec(c, query);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			logError("query failed: %s", PQresultErrorMessage(res));
			PQclear(res);
			PQfinish(c);
			/* XXX leak another connection? */
			exit(EXIT_FAILURE);
		}

		t->attributes[i].nseclabels = PQntuples(res);
		if (t->attributes[i].nseclabels > 0)
			t->attributes[i].seclabels = (PQLSecLabel *) malloc(t->attributes[i].nseclabels * sizeof(PQLSecLabel));
		else
			t->attributes[i].seclabels = NULL;

		logDebug("number of security labels in table %s.%s attribute %s: %d",
				 formatObjectIdentifier(t->obj.schemaname),
				 formatObjectIdentifier(t->obj.objectname),
				 t->attributes[i].attname,
				 t->attributes[i].nseclabels);

		for (j = 0; j < t->attributes[i].nseclabels; j++)
		{
			t->attributes[i].seclabels[j].provider = strdup(PQgetvalue(res, j, PQfnumber(res, "provider")));
			t->attributes[i].seclabels[j].label = strdup(PQgetvalue(res, j, PQfnumber(res, "label")));
		}

		PQclear(res);
	}
}

void
freeTables(PQLTable *t, int n)
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
			if (t[i].tbspcname)
				free(t[i].tbspcname);
			if (t[i].reloptions)
				free(t[i].reloptions);
			if (t[i].relreplidentidx)
				free(t[i].relreplidentidx);
			if (t[i].comment)
				free(t[i].comment);
			if (t[i].acl)
				free(t[i].acl);

			/* security labels */
			for (j = 0; j < t[i].nseclabels; j++)
			{
				free(t[i].seclabels[j].provider);
				free(t[i].seclabels[j].label);
			}

			if (t[i].seclabels)
				free(t[i].seclabels);

			/* attributes */
			for (j = 0; j < t[i].nattributes; j++)
			{
				int	k;

				free(t[i].attributes[j].attname);
				free(t[i].attributes[j].atttypname);
				if (t[i].attributes[j].attdefexpr)
					free(t[i].attributes[j].attdefexpr);
				if (t[i].attributes[j].attcollation)
					free(t[i].attributes[j].attcollation);
				if (t[i].attributes[j].attstorage)
					free(t[i].attributes[j].attstorage);
				if (t[i].attributes[j].attoptions)
					free(t[i].attributes[j].attoptions);
				if (t[i].attributes[j].comment)
					free(t[i].attributes[j].comment);

				/* security labels */
				for (k = 0; k < t[i].attributes[j].nseclabels; k++)
				{
					free(t[i].attributes[j].seclabels[k].provider);
					free(t[i].attributes[j].seclabels[k].label);
				}

				if (t[i].attributes[j].seclabels)
					free(t[i].attributes[j].seclabels);
			}

			/* check constraints */
			for (j = 0; j < t[i].ncheck; j++)
			{
				free(t[i].check[j].conname);
				free(t[i].check[j].condef);
				if (t[i].check[j].comment)
					free(t[i].check[j].comment);
			}

			/* foreign keys */
			for (j = 0; j < t[i].nfk; j++)
			{
				free(t[i].fk[j].conname);
				free(t[i].fk[j].condef);
				if (t[i].fk[j].comment)
					free(t[i].fk[j].comment);
			}

			/* primary key */
			if (t[i].pk.conname)
				free(t[i].pk.conname);
			if (t[i].pk.condef)
				free(t[i].pk.condef);
			if (t[i].pk.comment)
				free(t[i].pk.comment);

			/* ownedby sequences */
			for (j = 0; j < t[i].nownedby; j++)
			{
				free(t[i].seqownedby[j].schemaname);
				free(t[i].seqownedby[j].objectname);
				free(t[i].attownedby[j]);
			}

			if (t[i].attributes)
				free(t[i].attributes);
			if (t[i].check)
				free(t[i].check);
			if (t[i].fk)
				free(t[i].fk);
			if (t[i].seqownedby)
				free(t[i].seqownedby);
			if (t[i].attownedby)
				free(t[i].attownedby);
		}

		free(t);
	}
}

void
getOwnedBySequences(PGconn *c, PQLTable *t)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i;
	int			r;

	do
	{
		query = (char *) malloc(nquery * sizeof(char));

		r = snprintf(query, nquery,
					 "SELECT n.nspname, c.relname, a.attname FROM pg_depend d INNER JOIN pg_class c ON (c.oid = d.objid) INNER JOIN pg_namespace n ON (n.oid = c.relnamespace) INNER JOIN pg_attribute a ON (d.refobjid = a.attrelid AND d.refobjsubid = a.attnum) WHERE d.classid = 'pg_class'::regclass AND d.objsubid = 0 AND d.refobjid = %u AND d.refobjsubid != 0 AND d.deptype = 'a' AND c.relkind = 'S'",
					 t->obj.oid);

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

	t->nownedby = PQntuples(res);
	if (t->nownedby > 0)
	{
		t->seqownedby = (PQLObject *) malloc(t->nownedby * sizeof(PQLObject));
		t->attownedby = (char **) malloc(t->nownedby * sizeof(char *));
	}
	else
	{
		t->seqownedby = NULL;
		t->attownedby = NULL;
	}

	logDebug("number of sequences owned by the table %s.%s: %d",
			 formatObjectIdentifier(t->obj.schemaname),
			 formatObjectIdentifier(t->obj.objectname), t->nownedby);
	for (i = 0; i < t->nownedby; i++)
	{
		t->seqownedby[i].schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
											 "nspname")));
		t->seqownedby[i].objectname = strdup(PQgetvalue(res, i, PQfnumber(res,
											 "relname")));
		t->attownedby[i] = strdup(PQgetvalue(res, i, PQfnumber(res, "attname")));

		logDebug("sequence %s.%s owned by table %s.%s attribute %s",
				 formatObjectIdentifier(t->seqownedby[i].schemaname),
				 formatObjectIdentifier(t->seqownedby[i].objectname),
				 formatObjectIdentifier(t->obj.schemaname),
				 formatObjectIdentifier(t->obj.objectname),
				 t->attownedby[i]);
	}

	PQclear(res);
}

void
dumpDropTable(FILE *output, PQLTable t)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP TABLE %s.%s;",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname));
}

void
dumpCreateTable(FILE *output, PQLTable t)
{
	int		i;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE %sTABLE %s.%s (", t.unlogged ? "UNLOGGED " : "",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname));

	/* print attributes */
	for (i = 0; i < t.nattributes; i++)
	{
		if (i == 0)
			fprintf(output, "\n");
		else
			fprintf(output, ",\n");

		/* attribute name and type */
		fprintf(output, "%s %s", t.attributes[i].attname, t.attributes[i].atttypname);

		/* collate */
		/* XXX schema-qualified? */
		if (t.attributes[i].attcollation != NULL)
			fprintf(output, " COLLATE \"%s\"", t.attributes[i].attcollation);

		/* default value? */
		if (t.attributes[i].attdefexpr != NULL)
			fprintf(output, " DEFAULT %s", t.attributes[i].attdefexpr);

		/* not null? */
		if (t.attributes[i].attnotnull)
			fprintf(output, " NOT NULL");
	}

	/* print check constraints */
	for (i = 0; i < t.ncheck; i++)
	{
		fprintf(output, ",\n");
		fprintf(output, "CONSTRAINT %s %s", t.check[i].conname, t.check[i].condef);
	}

	fprintf(output, "\n)");

	/* reloptions */
	if (t.reloptions != NULL)
		fprintf(output, "\nWITH (%s)", t.reloptions);

	fprintf(output, ";");

	/* replica identity */
	if (t.relreplident != 'v')		/* 'v' (void) means < 9.4 */
	{

		switch (t.relreplident)
		{
			case 'n':
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY NOTHING;",
						formatObjectIdentifier(t.obj.schemaname),
						formatObjectIdentifier(t.obj.objectname));
				break;
			case 'd':
				/* print nothing. After all, it is the default */
				break;
			case 'f':
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY FULL;",
						formatObjectIdentifier(t.obj.schemaname),
						formatObjectIdentifier(t.obj.objectname));
				break;
			case 'i':
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY USING INDEX %s;",
						formatObjectIdentifier(t.obj.schemaname),
						formatObjectIdentifier(t.obj.objectname),
						formatObjectIdentifier(t.relreplidentidx));
				break;
			default:
				logWarning("replica identity %c is invalid", t.relreplident);
		}
	}

	/* statistics target */
	for (i = 0; i < t.nattributes; i++)
	{
		dumpAlterColumnSetStatistics(output, t, i, false);
		dumpAlterColumnSetStorage(output, t, i, false);
	}

	/* print primary key */
	if (t.pk.conname != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s\n",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname));
		fprintf(output, "\tADD CONSTRAINT %s %s", t.pk.conname, t.pk.condef);
		fprintf(output, ";");
	}

	/* print foreign key constraints */
	for (i = 0; i < t.nfk; i++)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s\n",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname));
		fprintf(output, "\tADD CONSTRAINT %s %s", t.fk[i].conname, t.fk[i].condef);
		fprintf(output, ";");
	}

	/* XXX Should it belong to sequence.c? */
	/* print owned by sequences */
	for (i = 0; i < t.nownedby; i++)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SEQUENCE %s.%s OWNED BY %s.%s",
				formatObjectIdentifier(t.seqownedby[i].schemaname),
				formatObjectIdentifier(t.seqownedby[i].objectname),
				formatObjectIdentifier(t.obj.objectname),
				formatObjectIdentifier(t.attownedby[i]));
		fprintf(output, ";");
	}

	/* comment */
	if (options.comment)
	{
		if (t.comment != NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TABLE %s.%s IS '%s';",
					formatObjectIdentifier(t.obj.schemaname),
					formatObjectIdentifier(t.obj.objectname),
					t.comment);
		}

		/* columns */
		for (i = 0; i < t.nattributes; i++)
		{
			if (t.attributes[i].comment != NULL)
			{
				fprintf(output, "\n\n");
				fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS '%s';",
						formatObjectIdentifier(t.obj.schemaname),
						formatObjectIdentifier(t.obj.objectname),
						formatObjectIdentifier(t.attributes[i].attname),
						t.attributes[i].comment);
			}
		}

		/* primary key */
		if (t.pk.comment != NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS '%s';",
					formatObjectIdentifier(t.pk.conname),
					formatObjectIdentifier(t.obj.schemaname),
					formatObjectIdentifier(t.obj.objectname),
					t.pk.comment);
		}

		/* foreign key */
		for (i = 0; i < t.nfk; i++)
		{
			if (t.fk[i].comment != NULL)
			{
				fprintf(output, "\n\n");
				fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS '%s';",
						formatObjectIdentifier(t.fk[i].conname),
						formatObjectIdentifier(t.obj.schemaname),
						formatObjectIdentifier(t.obj.objectname),
						t.fk[i].comment);
			}
		}

		/* check constraint */
		for (i = 0; i < t.ncheck; i++)
		{
			if (t.check[i].comment != NULL)
			{
				fprintf(output, "\n\n");
				fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS '%s';",
						formatObjectIdentifier(t.check[i].conname),
						formatObjectIdentifier(t.obj.schemaname),
						formatObjectIdentifier(t.obj.objectname),
						t.check[i].comment);
			}
		}
	}

	/* attribute options */
	for (i = 0; i < t.nattributes; i++)
	{
		if (t.attributes[i].attoptions)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET (%s)",
					formatObjectIdentifier(t.obj.schemaname),
					formatObjectIdentifier(t.obj.objectname),
					t.attributes[i].attname,
					t.attributes[i].attoptions);
		}
	}

	/* security labels */
	if (options.securitylabels && t.nseclabels > 0)
	{
		for (i = 0; i < t.nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS '%s';",
					t.seclabels[i].provider,
					formatObjectIdentifier(t.obj.schemaname),
					formatObjectIdentifier(t.obj.objectname),
					t.seclabels[i].label);
		}

		/* attributes */
		for (i = 0; i < t.nattributes; i++)
		{
			if (t.attributes[i].nseclabels > 0)
			{
				int	j;

				for (j = 0; j < t.attributes[i].nseclabels; j++)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS '%s';",
							t.attributes[i].seclabels[j].provider,
							formatObjectIdentifier(t.obj.schemaname),
							formatObjectIdentifier(t.obj.objectname),
							t.attributes[i].attname,
							t.attributes[i].seclabels[j].label);
				}
			}
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE %s.%s OWNER TO %s;",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				t.owner);
	}

	/* privileges */
	/* XXX second t.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TABLE, t.obj, t.obj, NULL, t.acl, NULL);
}

static void
dumpAddColumn(FILE *output, PQLTable t, int i)
{
	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s ADD COLUMN %s %s",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname), t.attributes[i].attname,
			t.attributes[i].atttypname);

	/* collate */
	/* XXX schema-qualified? */
	if (t.attributes[i].attcollation != NULL)
		fprintf(output, " COLLATE \"%s\"", t.attributes[i].attcollation);

	/* default value? */
	if (t.attributes[i].attdefexpr != NULL)
		fprintf(output, " DEFAULT %s", t.attributes[i].attdefexpr);

	/* not null? */
	/*
	 * XXX if the table already contains data it will fail miserably unless you
	 * XXX declare the DEFAULT clause. One day, this piece of code will be
	 * XXX smarter to warn the OP that postgres cannot automagically set not
	 * XXX null in a table that already contains data.
	 */
	if (t.attributes[i].attnotnull)
		fprintf(output, " NOT NULL");

	/* attribute options */
	if (t.attributes[i].attoptions)
		fprintf(output, " SET (%s)", t.attributes[i].attoptions);

	fprintf(output, ";");

	/* comment */
	if (options.comment && t.attributes[i].comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS '%s';",
				formatObjectIdentifier(t.obj.schemaname),
				formatObjectIdentifier(t.obj.objectname),
				formatObjectIdentifier(t.attributes[i].attname),
				t.attributes[i].comment);
	}

	/* security labels */
	if (options.securitylabels && t.attributes[i].nseclabels > 0)
	{
		int	j;

		for (j = 0; j < t.attributes[i].nseclabels; j++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS '%s';",
					t.attributes[i].seclabels[j].provider,
					formatObjectIdentifier(t.obj.schemaname),
					formatObjectIdentifier(t.obj.objectname),
					formatObjectIdentifier(t.attributes[i].attname),
					t.attributes[i].seclabels[j].label);
		}
	}
}

static void
dumpRemoveColumn(FILE *output, PQLTable t, int i)
{
	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s DROP COLUMN %s;",
			formatObjectIdentifier(t.obj.schemaname),
			formatObjectIdentifier(t.obj.objectname), t.attributes[i].attname);
}

static void
dumpAlterColumn(FILE *output, PQLTable a, int i, PQLTable b, int j)
{
	if (strcmp(a.attributes[i].atttypname, b.attributes[j].atttypname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET DATA TYPE %s",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname), b.attributes[j].attname,
				b.attributes[j].atttypname);

		/* collate */
		/* XXX schema-qualified? */
		if (b.attributes[j].attcollation != NULL)
			fprintf(output, " COLLATE \"%s\"", b.attributes[j].attcollation);

		fprintf(output, ";");
	}

	/* default value? */
	if (a.attributes[i].attdefexpr == NULL && b.attributes[j].attdefexpr != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET DEFAULT %s;",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname), b.attributes[j].attname,
				b.attributes[j].attdefexpr);
	}
	else if (a.attributes[i].attdefexpr != NULL &&
			 b.attributes[j].attdefexpr == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s DROP DEFAULT;",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname), b.attributes[j].attname);
	}

	/* not null? */
	if (!a.attributes[i].attnotnull && b.attributes[j].attnotnull)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET NOT NULL;",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname), b.attributes[j].attname);
	}
	else if (a.attributes[i].attnotnull && !b.attributes[j].attnotnull)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s DROP NOT NULL;",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname), b.attributes[j].attname);
	}

	/* comment */
	if (options.comment)
	{
		if ((a.attributes[i].comment == NULL && b.attributes[j].comment != NULL) ||
				(a.attributes[i].comment != NULL && b.attributes[j].comment != NULL &&
				 strcmp(a.attributes[i].comment, b.attributes[j].comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					formatObjectIdentifier(b.attributes[j].attname),
					b.attributes[j].comment);
		}
		else if (a.attributes[i].comment != NULL && b.attributes[j].comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS NULL;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					formatObjectIdentifier(b.attributes[j].attname));
		}
	}

	/* security labels */
	if (options.securitylabels)
	{
		if (a.attributes[i].seclabels == NULL && b.attributes[j].seclabels != NULL)
		{
			int	k;

			for (k = 0; k < b.attributes[j].nseclabels; k++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS '%s';",
						b.attributes[j].seclabels[k].provider,
						formatObjectIdentifier(b.obj.schemaname),
						formatObjectIdentifier(b.obj.objectname),
						formatObjectIdentifier(b.attributes[j].attname),
						b.attributes[j].seclabels[k].label);
			}
		}
		else if (a.attributes[i].seclabels != NULL && b.attributes[j].seclabels == NULL)
		{
			int	k;

			for (k = 0; k < a.nseclabels; k++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS NULL;",
						a.attributes[i].seclabels[k].provider,
						formatObjectIdentifier(a.obj.schemaname),
						formatObjectIdentifier(a.obj.objectname),
						formatObjectIdentifier(a.attributes[i].attname));
			}
		}
		else if (a.attributes[i].seclabels != NULL && b.attributes[j].seclabels != NULL)
		{
			int	k, l;

			k = l = 0;
			while (k < a.attributes[i].nseclabels || l < b.attributes[j].nseclabels)
			{
				if (k == a.attributes[i].nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS '%s';",
							b.attributes[j].seclabels[l].provider,
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname),
							formatObjectIdentifier(b.attributes[j].attname),
							b.attributes[j].seclabels[l].label);
					l++;
				}
				else if (l == b.attributes[j].nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS NULL;",
							a.attributes[i].seclabels[k].provider,
							formatObjectIdentifier(a.obj.schemaname),
							formatObjectIdentifier(a.obj.objectname),
							formatObjectIdentifier(a.attributes[i].attname));
					k++;
				}
				else if (strcmp(a.attributes[i].seclabels[k].provider, b.attributes[j].seclabels[l].provider) == 0)
				{
					if (strcmp(a.attributes[i].seclabels[k].label, b.attributes[j].seclabels[l].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS '%s';",
								b.attributes[j].seclabels[l].provider,
								formatObjectIdentifier(b.obj.schemaname),
								formatObjectIdentifier(b.obj.objectname),
								formatObjectIdentifier(b.attributes[j].attname),
								b.attributes[j].seclabels[l].label);
					}
					k++;
					l++;
				}
				else if (strcmp(a.attributes[i].seclabels[k].provider, b.attributes[j].seclabels[l].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS NULL;",
							a.attributes[i].seclabels[k].provider,
							formatObjectIdentifier(a.obj.schemaname),
							formatObjectIdentifier(a.obj.objectname),
							formatObjectIdentifier(a.attributes[i].attname));
					k++;
				}
				else if (strcmp(a.attributes[i].seclabels[k].provider, b.attributes[j].seclabels[l].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS '%s';",
							b.attributes[j].seclabels[l].provider,
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname),
							formatObjectIdentifier(b.attributes[j].attname),
							b.attributes[j].seclabels[l].label);
					l++;
				}
			}
		}
	}
}

static void
dumpAlterColumnSetStatistics(FILE *output, PQLTable a, int i, bool force)
{
	if (a.attributes[i].attstattarget != -1 || force)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET STATISTICS %d",
				formatObjectIdentifier(a.obj.schemaname),
				formatObjectIdentifier(a.obj.objectname),
				a.attributes[i].attname,
				a.attributes[i].attstattarget);
		fprintf(output, ";");
	}
}

static void
dumpAlterColumnSetStorage(FILE *output, PQLTable a, int i, bool force)
{
	if (!a.attributes[i].defstorage || force)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET STORAGE %s",
				formatObjectIdentifier(a.obj.schemaname),
				formatObjectIdentifier(a.obj.objectname),
				a.attributes[i].attname,
				a.attributes[i].attstorage);
		fprintf(output, ";");
	}
}

/*
 * Set attribute options if needed
 */
static void
dumpAlterColumnSetOptions(FILE *output, PQLTable a, int i, PQLTable b, int j)
{
	if (a.attributes[i].attoptions == NULL && b.attributes[j].attoptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET (%s)",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname),
				b.attributes[j].attname,
				b.attributes[j].attoptions);
	}
	else if (a.attributes[i].attoptions != NULL &&
			 b.attributes[j].attoptions == NULL)
	{
		stringList	*rlist;

		rlist = diffRelOptions(a.attributes[i].attoptions, b.attributes[j].attoptions,
							   PGQ_EXCEPT);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printRelOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s RESET (%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.attributes[j].attname,
					resetlist);
			fprintf(output, ";");

			free(resetlist);
			freeStringList(rlist);
		}
	}
	else if (a.attributes[i].attoptions != NULL &&
			 b.attributes[j].attoptions != NULL &&
			 strcmp(a.attributes[i].attoptions, b.attributes[j].attoptions) != 0)
	{
		stringList	*rlist, *slist;

		rlist = diffRelOptions(a.attributes[i].attoptions, b.attributes[j].attoptions,
							   PGQ_EXCEPT);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printRelOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s RESET (%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.attributes[j].attname,
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
		slist = buildRelOptions(b.attributes[j].attoptions);
		if (slist)
		{
			char	*setlist;

			setlist = printRelOptions(slist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE ONLY %s.%s ALTER COLUMN %s SET (%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.attributes[j].attname,
					setlist);
			fprintf(output, ";");

			free(setlist);
			freeStringList(slist);
		}
	}
}

void
dumpAlterTable(FILE *output, PQLTable a, PQLTable b)
{
	int i, j;

	/* the attributes are sorted by name */
	i = j = 0;
	while (i < a.nattributes || j < b.nattributes)
	{
		/*
		 * End of table a attributes. Additional columns from table b will be
		 * added.
		 */
		if (i == a.nattributes)
		{
			logDebug("table %s.%s attribute %s (%s) added",
					 formatObjectIdentifier(b.obj.schemaname),
					 formatObjectIdentifier(b.obj.objectname), b.attributes[j].attname,
					 b.attributes[j].atttypname);

			dumpAddColumn(output, b, j);

			dumpAlterColumnSetStatistics(output, b, j, false);	/* statistics target */
			dumpAlterColumnSetStorage(output, b, j, false);		/* storage */

			j++;
		}
		/*
		 * End of table b attributes. Additional columns from table a will be
		 * removed.
		 */
		else if (j == b.nattributes)
		{
			logDebug("table %s.%s attribute %s (%s) removed",
					 formatObjectIdentifier(a.obj.schemaname),
					 formatObjectIdentifier(a.obj.objectname), a.attributes[i].attname,
					 a.attributes[i].atttypname);

			dumpRemoveColumn(output, a, i);
			i++;
		}
		else if (strcmp(a.attributes[i].attname, b.attributes[j].attname) == 0)
		{
			/* same column name but different data types */
			/*
			 * XXX If we choose to DROP COLUMN follows by ADD COLUMN the data in
			 * XXX the old column will be discarded and won't have the chance to
			 * XXX be converted. That way, we try to convert between data types.
			 * XXX One day, this piece of code will be smarter to warn the OP
			 * XXX that postgres cannot automagically convert that column.
			 */
			if (strcmp(a.attributes[i].atttypname, b.attributes[j].atttypname) != 0 ||
					a.attributes[i].attnotnull != b.attributes[j].attnotnull)
			{
				logDebug("table %s.%s attribute %s (%s) altered to (%s)",
						 formatObjectIdentifier(a.obj.schemaname),
						 formatObjectIdentifier(a.obj.objectname), a.attributes[i].attname,
						 a.attributes[i].atttypname, b.attributes[j].atttypname);

				dumpAlterColumn(output, a, i, b, j);
			}

			/* do attribute options change? */
			dumpAlterColumnSetOptions(output, a, i, b, j);

			/* column statistics changed */
			if (a.attributes[i].attstattarget != b.attributes[j].attstattarget)
				dumpAlterColumnSetStatistics(output, b, j, true);

			/* storage changed */
			if (a.attributes[i].defstorage != b.attributes[j].defstorage)
				dumpAlterColumnSetStorage(output, b, j, true);

			i++;
			j++;
		}
		else if (strcmp(a.attributes[i].attname, b.attributes[j].attname) < 0)
		{
			logDebug("table %s.%s attribute %s (%s) removed",
					 formatObjectIdentifier(a.obj.schemaname),
					 formatObjectIdentifier(a.obj.objectname), a.attributes[i].attname,
					 a.attributes[i].atttypname);

			dumpRemoveColumn(output, a, i);
			i++;
		}
		else if (strcmp(a.attributes[i].attname, b.attributes[j].attname) > 0)
		{
			logDebug("table %s.%s attribute %s (%s) added",
					 formatObjectIdentifier(b.obj.schemaname),
					 formatObjectIdentifier(b.obj.objectname), b.attributes[j].attname,
					 b.attributes[j].atttypname);

			dumpAddColumn(output, b, j);

			dumpAlterColumnSetStatistics(output, b, j, false);	/* statistics target */
			dumpAlterColumnSetStorage(output, b, j, false);		/* storage */

			j++;
		}
	}

	/* reloptions */
	if ((a.reloptions == NULL && b.reloptions != NULL))
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE %s.%s SET (%s)",
				formatObjectIdentifier(b.obj.schemaname),
				formatObjectIdentifier(b.obj.objectname),
				b.reloptions);
		fprintf(output, ";");
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
			fprintf(output, "ALTER TABLE %s.%s RESET (%s)",
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
			fprintf(output, "ALTER TABLE %s.%s SET (%s)",
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
			fprintf(output, "ALTER TABLE %s.%s RESET (%s)",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					resetlist);
			fprintf(output, ";");

			free(resetlist);
			freeStringList(rlist);
		}
	}

	/*
	 * replica identity
	 *
	 * This feature is only emitted iif both servers support REPLICA IDENTITY.
	 * Otherwise, users will be warned.
	 */
	if (a.relreplident != 'v' && b.relreplident != 'v')
	{
		if (a.relreplident != b.relreplident)
		{
			switch (b.relreplident)
			{
				case 'n':
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY NOTHING;",
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname));
					break;
				case 'd':
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY DEFAULT;",
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname));
					break;
				case 'f':
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY FULL;",
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname));
					break;
				case 'i':
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY USING INDEX %s;",
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname),
							formatObjectIdentifier(b.relreplidentidx));
					break;
				default:
					logWarning("replica identity %c is invalid", b.relreplident);
			}
		}
	}
	else
	{
		logWarning("ignoring replica identity because some server does not support it");
	}

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TABLE %s.%s IS '%s';",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TABLE %s.%s IS NULL;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname));
		}
	}

	/* security labels */
	if (options.securitylabels)
	{
		if (a.seclabels == NULL && b.seclabels != NULL)
		{
			for (i = 0; i < b.nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS '%s';",
						b.seclabels[i].provider,
						formatObjectIdentifier(b.obj.schemaname),
						formatObjectIdentifier(b.obj.objectname),
						b.seclabels[i].label);
			}
		}
		else if (a.seclabels != NULL && b.seclabels == NULL)
		{
			for (i = 0; i < a.nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS NULL;",
						a.seclabels[i].provider,
						formatObjectIdentifier(a.obj.schemaname),
						formatObjectIdentifier(a.obj.objectname));
			}
		}
		else if (a.seclabels != NULL && b.seclabels != NULL)
		{
			i = j = 0;
			while (i < a.nseclabels || j < b.nseclabels)
			{
				if (i == a.nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS '%s';",
							b.seclabels[j].provider,
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname),
							b.seclabels[j].label);
					j++;
				}
				else if (j == b.nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS NULL;",
							a.seclabels[i].provider,
							formatObjectIdentifier(a.obj.schemaname),
							formatObjectIdentifier(a.obj.objectname));
					i++;
				}
				else if (strcmp(a.seclabels[i].provider, b.seclabels[j].provider) == 0)
				{
					if (strcmp(a.seclabels[i].label, b.seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS '%s';",
								b.seclabels[j].provider,
								formatObjectIdentifier(b.obj.schemaname),
								formatObjectIdentifier(b.obj.objectname),
								b.seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a.seclabels[i].provider, b.seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS NULL;",
							a.seclabels[i].provider,
							formatObjectIdentifier(a.obj.schemaname),
							formatObjectIdentifier(a.obj.objectname));
					i++;
				}
				else if (strcmp(a.seclabels[i].provider, b.seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON TABLE %s.%s IS '%s';",
							b.seclabels[j].provider,
							formatObjectIdentifier(b.obj.schemaname),
							formatObjectIdentifier(b.obj.objectname),
							b.seclabels[j].label);
					j++;
				}
			}
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a.owner, b.owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE %s.%s OWNER TO %s;",
					formatObjectIdentifier(b.obj.schemaname),
					formatObjectIdentifier(b.obj.objectname),
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TABLE, a.obj, b.obj, a.acl, b.acl, NULL);
	}
}
