/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * table.c
 *     Generate TABLE / FOREIGN TABLE commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE TABLE
 * DROP TABLE
 * ALTER TABLE
 * COMMENT ON TABLE
 * COMMENT ON COLUMN
 * COMMENT ON CONSTRAINT
 *
 * CREATE FOREIGN TABLE
 * DROP FOREIGN TABLE
 * ALTER FOREIGN TABLE
 * COMMENT ON FOREIGN TABLE
 *
 * TODO
 *
 * CREATE TABLE ... INHERITS
 * CREATE TABLE ... TABLESPACE
 * CREATE TABLE ... EXCLUDE
 * CREATE TABLE ... GENERATED ... AS IDENTITY
 *
 * CREATE FOREIGN TABLE ... INHERITS
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
 * ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION
 * ALTER TABLE ... ALTER COLUMN ... ADD GENERATED
 * ALTER TABLE ... ALTER COLUMN ... SET GENERATED
 * ALTER TABLE ... ALTER COLUMN ... DROP IDENTITY
 * ALTER TABLE ... ALTER COLUMN ... SET COMPRESSION
 *
 * ALTER FOREIGN TABLE ... RENAME COLUMN ... TO
 * ALTER FOREIGN TABLE ... RENAME TO
 * ALTER FOREIGN TABLE ... SET SCHEMA TO
 * ALTER FOREIGN TABLE ... INHERIT parent_table
 * ALTER FOREIGN TABLE ... NOINHERIT parent_table
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "table.h"


#define	PGQ_IS_REGULAR_TABLE(ptr) (ptr == 'r')
#define	PGQ_IS_PARTITIONED_TABLE(ptr) (ptr == 'p')
#define	PGQ_IS_FOREIGN_TABLE(ptr) (ptr == 'f')
#define	PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(ptr) \
	(PGQ_IS_REGULAR_TABLE(ptr) || PGQ_IS_PARTITIONED_TABLE(ptr))


static PQLTable *getTables(PGconn *c, int *n, char k);
static void getParentTables(PGconn *c, PQLTable *t);
static void dumpAddColumn(FILE *output, PQLTable *t, int i);
static void dumpRemoveColumn(FILE *output, PQLTable *t, int i);
static void dumpAlterColumn(FILE *output, PQLTable *a, int i, PQLTable *b,
							int j);
static void dumpAlterColumnSetStatistics(FILE *output, PQLTable *a, int i,
		bool force);
static void dumpAlterColumnSetStorage(FILE *output, PQLTable *a, int i,
									  bool force);
static void dumpAlterColumnSetOptions(FILE *output, PQLTable *a, int i,
									  PQLTable *b, int j);
static void dumpAddPK(FILE *output, PQLTable *t);
static void dumpRemovePK(FILE *output, PQLTable *t);
static void dumpAddFK(FILE *output, PQLTable *t, int i);
static void dumpRemoveFK(FILE *output, PQLTable *t, int i);
static void dumpAttachPartition(FILE *output, PQLTable *a);
static void dumpDetachPartition(FILE *output, PQLTable *a);

PQLTable *
getTables(PGconn *c, int *n, char k)
{
	PQLTable	*t;
	char		*query = NULL;
	PGresult	*res;
	int			i;
	char		*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(k))
		kind = strdup("table");
	else if (PGQ_IS_FOREIGN_TABLE(k))
		kind = strdup("foreign table");
	else
	{
		logError("kind is not a regular, partitioned or foreign table");
		exit(EXIT_FAILURE);
	}

	logNoise("%s: server version: %d", kind, PQserverVersion(c));

	/* FIXME relpersistence (9.1)? */
	/*
	 * XXX Using 'v' (void) to represent unsupported replica identity
	 */
	if (PQserverVersion(c) >= 100000)
	{
		if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(k))
		{
			query = psprintf("SELECT c.oid, n.nspname, c.relname, c.relkind, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, relreplident, reloftype, o.nspname AS typnspname, y.typname, c.relispartition, pg_get_partkeydef(c.oid) AS partitionkeydef, pg_get_expr(c.relpartbound, c.oid) AS partitionbound, c.relhassubclass FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) LEFT JOIN (pg_type y INNER JOIN pg_namespace o ON (y.typnamespace = o.oid)) ON (c.reloftype = y.oid) WHERE relkind IN ('r', 'p') AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, relname", include_schema_str, exclude_schema_str);
		}
		else if (PGQ_IS_FOREIGN_TABLE(k))
		{
			query = psprintf("SELECT c.oid, n.nspname, c.relname, c.relkind, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, relreplident, reloftype, o.nspname AS typnspname, y.typname, c.relispartition, pg_get_partkeydef(c.oid) AS partitionkeydef, pg_get_expr(c.relpartbound, c.oid) AS partitionbound, c.relhassubclass FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) LEFT JOIN (pg_type y INNER JOIN pg_namespace o ON (y.typnamespace = o.oid)) ON (c.reloftype = y.oid) WHERE relkind = 'f' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, relname", include_schema_str, exclude_schema_str);
		}
		else
		{
			logError("it is not a table or foreign table");
			exit(EXIT_FAILURE);
		}
	}
	else if (PQserverVersion(c) >= 90400)
	{
		if (PGQ_IS_REGULAR_TABLE(k))
		{
			query = psprintf("SELECT c.oid, n.nspname, c.relname, c.relkind, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, relreplident, reloftype, o.nspname AS typnspname, y.typname, false AS relispartition, NULL AS partitionkeydef, NULL AS partitionbound, c.relhassubclass FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) LEFT JOIN (pg_type y INNER JOIN pg_namespace o ON (y.typnamespace = o.oid)) ON (c.reloftype = y.oid) WHERE relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, relname", include_schema_str, exclude_schema_str);
		}
		else if (PGQ_IS_FOREIGN_TABLE(k))
		{
			query = psprintf("SELECT c.oid, n.nspname, c.relname, c.relkind, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, relreplident, reloftype, o.nspname AS typnspname, y.typname, false AS relispartition, NULL AS partitionkeydef, NULL AS partitionbound, c.relhassubclass FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) LEFT JOIN (pg_type y INNER JOIN pg_namespace o ON (y.typnamespace = o.oid)) ON (c.reloftype = y.oid) WHERE relkind = 'f' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, relname", include_schema_str, exclude_schema_str);
		}
		else
		{
			logError("it is not a table or foreign table");
			exit(EXIT_FAILURE);
		}
	}
	else if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		if (PGQ_IS_REGULAR_TABLE(k))
		{
			query = psprintf("SELECT c.oid, n.nspname, c.relname, c.relkind, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, 'v' AS relreplident, reloftype, o.nspname AS typnspname, y.typname, false AS relispartition, NULL AS partitionkeydef, NULL AS partitionbound, c.relhassubclass FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) LEFT JOIN (pg_type y INNER JOIN pg_namespace o ON (y.typnamespace = o.oid)) ON (c.reloftype = y.oid) WHERE relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, relname", include_schema_str, exclude_schema_str);
		}
		else if (PGQ_IS_FOREIGN_TABLE(k))
		{
			query = psprintf("SELECT c.oid, n.nspname, c.relname, c.relkind, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, 'v' AS relreplident, reloftype, o.nspname AS typnspname, y.typname, false AS relispartition, NULL AS partitionkeydef, NULL AS partitionbound, c.relhassubclass FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) LEFT JOIN (pg_type y INNER JOIN pg_namespace o ON (y.typnamespace = o.oid)) ON (c.reloftype = y.oid) WHERE relkind = 'f' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE t.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, relname", include_schema_str, exclude_schema_str);
		}
		else
		{
			logError("it is not a table or foreign table");
			exit(EXIT_FAILURE);
		}
	}
	else
	{
		if (PGQ_IS_REGULAR_TABLE(k))
		{
			query = psprintf("SELECT c.oid, n.nspname, c.relname, c.relkind, t.spcname AS tablespacename, 'p' AS relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl, 'v' AS relreplident, 0 AS reloftype, NULL AS typnspname, NULL AS typname, false AS relispartition, NULL AS partitionkeydef, NULL AS partitionbound, c.relhassubclass FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' %s%s ORDER BY n.nspname, relname", include_schema_str, exclude_schema_str);
		}
		else
		{
			logError("this version does not support foreign table");
			exit(EXIT_FAILURE);
		}
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
		t = (PQLTable *) malloc(*n * sizeof(PQLTable));
	else
		t = NULL;

	logDebug("number of %ss in server: %d", kind, *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		t[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
		t[i].kind = PQgetvalue(res, i, PQfnumber(res, "relkind"))[0];
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
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			t[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (t[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		t[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "relowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "relacl")))
			t[i].acl = NULL;
		else
			t[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "relacl")));

		t[i].relreplident = *(PQgetvalue(res, i, PQfnumber(res, "relreplident")));
		/* assigned iif REPLICA IDENTITY USING INDEX; see getTableAttributes() */
		t[i].relreplidentidx = NULL;

		if (PQgetisnull(res, i, PQfnumber(res, "typname")))
		{
			t[i].reloftype.oid = InvalidOid;
			t[i].reloftype.schemaname = NULL;
			t[i].reloftype.objectname = NULL;
		}
		else
		{
			t[i].reloftype.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "reloftype")),
										 NULL, 10);
			t[i].reloftype.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
											   "typnspname")));
			t[i].reloftype.objectname = strdup(PQgetvalue(res, i, PQfnumber(res,
											   "typname")));
		}

		if (PGQ_IS_PARTITIONED_TABLE(t[i].kind))
			t[i].partitionkey = strdup(PQgetvalue(res, i, PQfnumber(res,
												  "partitionkeydef")));
		else
			t[i].partitionkey = NULL;

		t[i].partition = (PQgetvalue(res, i, PQfnumber(res,
									 "relispartition"))[0] == 't');
		if (t[i].partition)
		{
			t[i].partitionbound = strdup(PQgetvalue(res, i, PQfnumber(res,
													"partitionbound")));
			getParentTables(c, &t[i]);
		}
		else
		{
			t[i].partitionbound = NULL;
			t[i].nparent = 0;
			t[i].parent = NULL;
		}

		/*
		 * Foreign table properties are not assigned here (see
		 * getForeignTableProperties), but default values are essential to
		 * avoid having trouble in freeTables.
		 */
		t[i].servername = NULL;
		t[i].ftoptions = NULL;

		/*
		 * Security labels are not assigned here (see getTableSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeTables.
		 */
		t[i].nseclabels = 0;
		t[i].seclabels = NULL;

		logDebug("%s \"%s\".\"%s\"", kind, t[i].obj.schemaname, t[i].obj.objectname);
	}

	PQclear(res);
	free(kind);

	return t;
}

PQLTable *
getRegularTables(PGconn *c, int *n)
{
	return getTables(c, n, 'r');
}

PQLTable *
getForeignTables(PGconn *c, int *n)
{
	return getTables(c, n, 'f');
}

static void
getParentTables(PGconn *c, PQLTable *t)
{
	char		*query;
	PGresult	*res;
	int			j;

	query = psprintf("SELECT c.oid, n.nspname, c.relname FROM pg_inherits i INNER JOIN pg_class c ON (c.oid = i.inhparent) INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE inhrelid = %u ORDER BY nspname, relname", t->obj.oid);

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

	t->nparent = PQntuples(res);
	if (t->nparent > 0)
		t->parent = (PQLObject *) malloc(t->nparent * sizeof(PQLObject));
	else
		t->parent = NULL;

	if (t->partition && t->nparent > 1)
	{
		logError("partition cannot have more than one parent table");
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	logDebug("number of parents from table \"%s\".\"%s\": %d",
			 t->obj.schemaname, t->obj.objectname, t->nparent);
	for (j = 0; j < t->nparent; j++)
	{
		t->parent[j].oid = strtoul(PQgetvalue(res, j, PQfnumber(res, "oid")), NULL, 10);
		t->parent[j].schemaname = strdup(PQgetvalue(res, j, PQfnumber(res, "nspname")));
		t->parent[j].objectname = strdup(PQgetvalue(res, j, PQfnumber(res, "relname")));
	}

	PQclear(res);
}

void
getForeignTableProperties(PGconn *c, PQLTable *t, int n)
{
	char		*query;
	PGresult	*res;
	int			i;

	for (i = 0; i < n; i++)
	{
		query = psprintf("SELECT s.srvname, array_to_string(f.ftoptions, ', ') AS ftoptions FROM pg_foreign_table f INNER JOIN pg_foreign_server s ON (f.ftserver = s.oid) WHERE f.ftrelid = %u", t[i].obj.oid);

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

		if (PQntuples(res) == 1)
		{
			t[i].servername = strdup(PQgetvalue(res, 0, PQfnumber(res, "srvname")));
			if (PQgetisnull(res, 0, PQfnumber(res, "ftoptions")))
				t->ftoptions = NULL;
			else
				t[i].ftoptions = strdup(PQgetvalue(res, 0, PQfnumber(res, "ftoptions")));
		}
		else
		{
			logError("foreign table \"%s\".\"%s\" has more than one entry",
					 t[i].obj.schemaname, t[i].obj.objectname);
			PQclear(res);
			PQfinish(c);
			/* XXX leak another connection? */
			exit(EXIT_FAILURE);
		}

		PQclear(res);
	}
}

void
getCheckConstraints(PGconn *c, PQLTable *t, int n)
{
	char		*query;
	PGresult	*res;
	int			i, j;
	char		*kind;

	for (i = 0; i < n; i++)
	{
		if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t[i].kind))
			kind = strdup("table");
		else if (PGQ_IS_FOREIGN_TABLE(t[i].kind))
			kind = strdup("foreign table");
		else
		{
			logError("kind is not a regular, partitioned or foreign table");
			exit(EXIT_FAILURE);
		}

		/* FIXME conislocal (8.4)? convalidated (9.2)? */
		/* XXX contype = 'c' needed? */
		query = psprintf("SELECT conname, pg_get_constraintdef(c.oid) AS condef, obj_description(c.oid, 'pg_constraint') AS description FROM pg_constraint c WHERE conrelid = %u AND contype = 'c' ORDER BY conname", t[i].obj.oid);

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

		t[i].ncheck = PQntuples(res);
		if (t[i].ncheck > 0)
			t[i].check = (PQLConstraint *) malloc(t[i].ncheck * sizeof(PQLConstraint));
		else
			t[i].check = NULL;

		logDebug("number of check constraints in %s \"%s\".\"%s\": %d",
				 kind, t[i].obj.schemaname, t[i].obj.objectname, t[i].ncheck);

		for (j = 0; j < t[i].ncheck; j++)
		{
			char	*withoutescape;

			t[i].check[j].conname = strdup(PQgetvalue(res, j, PQfnumber(res, "conname")));
			t[i].check[j].condef = strdup(PQgetvalue(res, j, PQfnumber(res, "condef")));
			if (PQgetisnull(res, j, PQfnumber(res, "description")))
				t[i].check[j].comment = NULL;
			else
			{
				withoutescape = PQgetvalue(res, j, PQfnumber(res, "description"));
				t[i].check[j].comment = PQescapeLiteral(c, withoutescape,
														strlen(withoutescape));
				if (t[i].check[j].comment == NULL)
				{
					logError("escaping comment failed: %s", PQerrorMessage(c));
					PQclear(res);
					PQfinish(c);
					/* XXX leak another connection? */
					exit(EXIT_FAILURE);
				}
			}
		}

		free(kind);

		PQclear(res);
	}
}

void
getFKConstraints(PGconn *c, PQLTable *t, int n)
{
	char		*query;
	PGresult	*res;
	int			i, j;

	for (i = 0; i < n; i++)
	{
		query = psprintf("SELECT conname, pg_get_constraintdef(c.oid) AS condef, obj_description(c.oid, 'pg_constraint') AS description FROM pg_constraint c WHERE conrelid = %u AND contype = 'f' ORDER BY conname", t[i].obj.oid);

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

		t[i].nfk = PQntuples(res);
		if (t[i].nfk > 0)
			t[i].fk = (PQLConstraint *) malloc(t[i].nfk * sizeof(PQLConstraint));
		else
			t[i].fk = NULL;

		logDebug("number of FK constraints in table \"%s\".\"%s\": %d",
				 t[i].obj.schemaname, t[i].obj.objectname, t[i].nfk);
		for (j = 0; j < t[i].nfk; j++)
		{
			char	*withoutescape;

			t[i].fk[j].conname = strdup(PQgetvalue(res, j, PQfnumber(res, "conname")));
			t[i].fk[j].condef = strdup(PQgetvalue(res, j, PQfnumber(res, "condef")));
			if (PQgetisnull(res, j, PQfnumber(res, "description")))
				t[i].fk[j].comment = NULL;
			else
			{
				withoutescape = PQgetvalue(res, j, PQfnumber(res, "description"));
				t[i].fk[j].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
				if (t[i].fk[j].comment == NULL)
				{
					logError("escaping comment failed: %s", PQerrorMessage(c));
					PQclear(res);
					PQfinish(c);
					/* XXX leak another connection? */
					exit(EXIT_FAILURE);
				}
			}
		}

		PQclear(res);
	}
}

void
getPKConstraints(PGconn *c, PQLTable *t, int n)
{
	char		*query;
	PGresult	*res;
	int			i;

	for (i = 0; i < n; i++)
	{
		/* XXX only 9.0+ */
		query = psprintf("SELECT conname, pg_get_constraintdef(c.oid) AS condef, obj_description(c.oid, 'pg_constraint') AS description FROM pg_constraint c WHERE conrelid = %u AND contype = 'p' ORDER BY conname", t[i].obj.oid);

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

		if (PQntuples(res) == 1)
		{
			char	*withoutescape;

			t[i].pk.conname = strdup(PQgetvalue(res, 0, PQfnumber(res, "conname")));
			t[i].pk.condef = strdup(PQgetvalue(res, 0, PQfnumber(res, "condef")));
			if (PQgetisnull(res, 0, PQfnumber(res, "description")))
				t[i].pk.comment = NULL;
			else
			{
				withoutescape = PQgetvalue(res, 0, PQfnumber(res, "description"));
				t[i].pk.comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
				if (t[i].pk.comment == NULL)
				{
					logError("escaping comment failed: %s", PQerrorMessage(c));
					PQclear(res);
					PQfinish(c);
					/* XXX leak another connection? */
					exit(EXIT_FAILURE);
				}
			}
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
	char		*query;
	PGresult	*res;
	int			i;
	char		*kind = NULL;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("table");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("foreign table");
	else
	{
		logError("kind is not a regular, partitioned or foreign table");
		exit(EXIT_FAILURE);
	}

	if (PQserverVersion(c) >=
			90200)	/* support for foreign table attribute options */
	{
		query = psprintf(
						  "SELECT a.attnum, a.attname, a.attnotnull, pg_catalog.format_type(t.oid, a.atttypmod) as atttypname, pg_get_expr(d.adbin, a.attrelid) as attdefexpr, CASE WHEN a.attcollation <> t.typcollation THEN c.collname ELSE NULL END AS attcollation, col_description(a.attrelid, a.attnum) AS description, a.attstattarget, a.attstorage, CASE WHEN t.typstorage <> a.attstorage THEN FALSE ELSE TRUE END AS defstorage, array_to_string(attoptions, ', ') AS attoptions, array_to_string(attfdwoptions, ', ') AS attfdwoptions, attacl FROM pg_attribute a LEFT JOIN pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum) LEFT JOIN pg_collation c ON (a.attcollation = c.oid) WHERE a.attrelid = %u AND a.attnum > 0 AND attisdropped IS FALSE ORDER BY a.attname",
						  t->obj.oid);
	}
	else if (PQserverVersion(c) >= 90100)	/* support for collation */
	{
		query = psprintf(
						  "SELECT a.attnum, a.attname, a.attnotnull, pg_catalog.format_type(t.oid, a.atttypmod) as atttypname, pg_get_expr(d.adbin, a.attrelid) as attdefexpr, CASE WHEN a.attcollation <> t.typcollation THEN c.collname ELSE NULL END AS attcollation, col_description(a.attrelid, a.attnum) AS description, a.attstattarget, a.attstorage, CASE WHEN t.typstorage <> a.attstorage THEN FALSE ELSE TRUE END AS defstorage, array_to_string(attoptions, ', ') AS attoptions, NULL AS attfdwoptions, attacl FROM pg_attribute a LEFT JOIN pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum) LEFT JOIN pg_collation c ON (a.attcollation = c.oid) WHERE a.attrelid = %u AND a.attnum > 0 AND attisdropped IS FALSE ORDER BY a.attname",
						  t->obj.oid);
	}
	else
	{
		query = psprintf(
						  "SELECT a.attnum, a.attname, a.attnotnull, pg_catalog.format_type(t.oid, a.atttypmod) as atttypname, pg_get_expr(d.adbin, a.attrelid) as attdefexpr, NULL AS attcollation, col_description(a.attrelid, a.attnum) AS description, a.attstattarget, a.attstorage, CASE WHEN t.typstorage <> a.attstorage THEN FALSE ELSE TRUE END AS defstorage, array_to_string(attoptions, ', ') AS attoptions, NULL AS attfdwoptions, attacl FROM pg_attribute a LEFT JOIN pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum) WHERE a.attrelid = %u AND a.attnum > 0 AND attisdropped IS FALSE ORDER BY a.attname",
						  t->obj.oid);
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

	t->nattributes = PQntuples(res);
	if (t->nattributes > 0)
		t->attributes = (PQLAttribute *) malloc(t->nattributes * sizeof(PQLAttribute));
	else
		t->attributes = NULL;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
	{
		logDebug("number of attributes in table \"%s\".\"%s\": %d", t->obj.schemaname,
				 t->obj.objectname, t->nattributes);

		/* reloptions is only available for regular tables */
		if (t->reloptions)
			logDebug("table \"%s\".\"%s\": reloptions: %s", t->obj.schemaname,
					 t->obj.objectname, t->reloptions);
		else
			logDebug("table \"%s\".\"%s\": no reloptions", t->obj.schemaname,
					 t->obj.objectname);
	}
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
	{
		logDebug("number of attributes in foreign table \"%s\".\"%s\": %d",
				 t->obj.schemaname,
				 t->obj.objectname, t->nattributes);
	}

	for (i = 0; i < t->nattributes; i++)
	{
		char	storage;
		char	*withoutescape;

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

		/* attribute FDW options */
		if (PQgetisnull(res, i, PQfnumber(res, "attfdwoptions")))
			t->attributes[i].attfdwoptions = NULL;
		else
			t->attributes[i].attfdwoptions = strdup(PQgetvalue(res, i, PQfnumber(res,
													"attfdwoptions")));

		/* attribute ACL */
		if (PQgetisnull(res, i, PQfnumber(res, "attacl")))
			t->attributes[i].acl = NULL;
		else
			t->attributes[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "attacl")));

		/* comment */
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t->attributes[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			t->attributes[i].comment = PQescapeLiteral(c, withoutescape,
									   strlen(withoutescape));
			if (t->attributes[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		/*
		 * Security labels are not assigned here (see getTableSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeTables.
		 */
		t->attributes[i].nseclabels = 0;
		t->attributes[i].seclabels = NULL;

		if (t->attributes[i].attdefexpr != NULL)
			logDebug("table: \"%s\".\"%s\" ; attribute \"%s\"; type: %s ; default: %s ; storage: %s",
					 t->obj.schemaname, t->obj.objectname, t->attributes[i].attname,
					 t->attributes[i].atttypname, t->attributes[i].attdefexpr,
					 t->attributes[i].attstorage);
		else
			logDebug("table: \"%s\".\"%s\" ; attribute \"%s\"; type: %s ; storage: %s",
					 t->obj.schemaname, t->obj.objectname, t->attributes[i].attname,
					 t->attributes[i].atttypname,
					 t->attributes[i].attstorage);
	}

	PQclear(res);

	/* replica identity using index */
	if (t->relreplident == 'i')
	{
		query = psprintf("SELECT c.relname AS idxname FROM pg_index i INNER JOIN pg_class c ON (i.indexrelid = c.oid) WHERE indrelid = %u AND indisreplident", t->obj.oid);

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

		i  = PQntuples(res);
		if (i == 1)
		{
			t->relreplidentidx = strdup(PQgetvalue(res, 0, PQfnumber(res,
												   "idxname")));
		}
		else
			logWarning("table \"%s\".\"%s\" should contain one replica identity index (returned %d)",
					   t->obj.schemaname, t->obj.objectname, i);

		PQclear(res);
	}

	if (kind)
		free(kind);
}

void
getTableSecurityLabels(PGconn *c, PQLTable *t)
{
	char		*query;
	PGresult	*res;
	int			i;
	char		*kind = NULL;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("table");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("foreign table");
	else
	{
		logError("kind is not a regular, partitioned or foreign table");
		exit(EXIT_FAILURE);
	}

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		if (kind)
			free(kind);
		return;
	}

	query = psprintf("SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_class' AND s.objoid = %u ORDER BY provider", t->obj.oid);

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

	t->nseclabels = PQntuples(res);
	if (t->nseclabels > 0)
		t->seclabels = (PQLSecLabel *) malloc(t->nseclabels * sizeof(PQLSecLabel));
	else
		t->seclabels = NULL;

	logDebug("number of security labels in %s \"%s\".\"%s\": %d",
			 kind, t->obj.schemaname, t->obj.objectname, t->nseclabels);

	for (i = 0; i < t->nseclabels; i++)
	{
		char	*withoutescape;

		t->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		withoutescape = PQgetvalue(res, i, PQfnumber(res, "label"));
		t->seclabels[i].label = PQescapeLiteral(c, withoutescape,
												strlen(withoutescape));
		if (t->seclabels[i].label == NULL)
		{
			logError("escaping label failed: %s", PQerrorMessage(c));
			PQclear(res);
			PQfinish(c);
			/* XXX leak another connection? */
			exit(EXIT_FAILURE);
		}
	}

	PQclear(res);

	/* attributes */
	for (i = 0; i < t->nattributes; i++)
	{
		int		j;

		query = psprintf("SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_attribute' AND s.objoid = %u AND s.objsubid = %u ORDER BY provider", t->obj.oid, t->attributes[i].attnum);

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

		t->attributes[i].nseclabels = PQntuples(res);
		if (t->attributes[i].nseclabels > 0)
			t->attributes[i].seclabels = (PQLSecLabel *) malloc(t->attributes[i].nseclabels
										 * sizeof(PQLSecLabel));
		else
			t->attributes[i].seclabels = NULL;

		logDebug("number of security labels in %s \"%s\".\"%s\" attribute \"%s\": %d",
				 kind, t->obj.schemaname, t->obj.objectname,
				 t->attributes[i].attname, t->attributes[i].nseclabels);

		for (j = 0; j < t->attributes[i].nseclabels; j++)
		{
			char	*withoutescape;

			t->attributes[i].seclabels[j].provider = strdup(PQgetvalue(res, j,
					PQfnumber(res, "provider")));
			withoutescape = PQgetvalue(res, j, PQfnumber(res, "label"));
			t->attributes[i].seclabels[j].label = PQescapeLiteral(c, withoutescape,
												  strlen(withoutescape));
			if (t->attributes[i].seclabels[j].label == NULL)
			{
				logError("escaping label failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		PQclear(res);
	}

	free(kind);
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
			if (t[i].reloftype.schemaname)
				free(t[i].reloftype.schemaname);
			if (t[i].reloftype.objectname)
				free(t[i].reloftype.objectname);
			if (t[i].partitionkey)
				free(t[i].partitionkey);
			if (t[i].partitionbound)
				free(t[i].partitionbound);
			if (t[i].servername)
				free(t[i].servername);
			if (t[i].ftoptions)
				free(t[i].ftoptions);
			if (t[i].comment)
				PQfreemem(t[i].comment);
			if (t[i].acl)
				free(t[i].acl);

			/* security labels */
			for (j = 0; j < t[i].nseclabels; j++)
			{
				free(t[i].seclabels[j].provider);
				PQfreemem(t[i].seclabels[j].label);
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
				if (t[i].attributes[j].acl)
					free(t[i].attributes[j].acl);
				if (t[i].attributes[j].comment)
					PQfreemem(t[i].attributes[j].comment);

				/* security labels */
				for (k = 0; k < t[i].attributes[j].nseclabels; k++)
				{
					free(t[i].attributes[j].seclabels[k].provider);
					PQfreemem(t[i].attributes[j].seclabels[k].label);
				}

				if (t[i].attributes[j].seclabels)
					free(t[i].attributes[j].seclabels);
			}

			/* parent tables */
			for (j = 0; j < t[i].nparent; j++)
			{
				free(t[i].parent[j].schemaname);
				free(t[i].parent[j].objectname);
			}

			/* check constraints */
			for (j = 0; j < t[i].ncheck; j++)
			{
				free(t[i].check[j].conname);
				free(t[i].check[j].condef);
				if (t[i].check[j].comment)
					PQfreemem(t[i].check[j].comment);
			}

			/* foreign keys */
			for (j = 0; j < t[i].nfk; j++)
			{
				free(t[i].fk[j].conname);
				free(t[i].fk[j].condef);
				if (t[i].fk[j].comment)
					PQfreemem(t[i].fk[j].comment);
			}

			/* primary key */
			if (t[i].pk.conname)
				free(t[i].pk.conname);
			if (t[i].pk.condef)
				free(t[i].pk.condef);
			if (t[i].pk.comment)
				PQfreemem(t[i].pk.comment);

			/* ownedby sequences */
			for (j = 0; j < t[i].nownedby; j++)
			{
				free(t[i].seqownedby[j].schemaname);
				free(t[i].seqownedby[j].objectname);
				free(t[i].attownedby[j]);
			}

			if (t[i].attributes)
				free(t[i].attributes);
			if (t[i].parent)
				free(t[i].parent);
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
	char		*query;
	PGresult	*res;
	int			i;

	query = psprintf("SELECT n.nspname, c.relname, a.attname FROM pg_depend d INNER JOIN pg_class c ON (c.oid = d.objid) INNER JOIN pg_namespace n ON (n.oid = c.relnamespace) INNER JOIN pg_attribute a ON (d.refobjid = a.attrelid AND d.refobjsubid = a.attnum) WHERE d.classid = 'pg_class'::regclass AND d.objsubid = 0 AND d.refobjid = %u AND d.refobjsubid != 0 AND d.deptype = 'a' AND c.relkind = 'S'", t->obj.oid);

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

	logDebug("number of sequences owned by the table \"%s\".\"%s\": %d",
			 t->obj.schemaname, t->obj.objectname, t->nownedby);
	for (i = 0; i < t->nownedby; i++)
	{
		t->seqownedby[i].schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
											 "nspname")));
		t->seqownedby[i].objectname = strdup(PQgetvalue(res, i, PQfnumber(res,
											 "relname")));
		t->attownedby[i] = strdup(PQgetvalue(res, i, PQfnumber(res, "attname")));

		logDebug("sequence \"%s\".\"%s\" owned by table \"%s\".\"%s\" attribute \"%s\"",
				 t->seqownedby[i].schemaname, t->seqownedby[i].objectname,
				 t->obj.schemaname, t->obj.objectname, t->attownedby[i]);
	}

	PQclear(res);
}

void
dumpDropTable(FILE *output, PQLTable *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);
	char	*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	fprintf(output, "\n\n");
	fprintf(output, "DROP %s %s.%s;", kind, schema, tabname);

	free(kind);
	free(schema);
	free(tabname);
}

void
dumpCreateTable(FILE *output, FILE *output2, PQLTable *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);
	char	*kind;

	char	*typeschema;
	char	*typename;

	int		i;
	bool	hasatts = false;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	fprintf(output, "\n\n");
	fprintf(output, "CREATE %s%s %s.%s ", t->unlogged ? "UNLOGGED " : "", kind,
			schema,
			tabname);

	/* typed table */
	if (t->reloftype.oid != InvalidOid)
	{
		typeschema = formatObjectIdentifier(t->reloftype.schemaname);
		typename = formatObjectIdentifier(t->reloftype.objectname);

		fprintf(output, "OF %s.%s", typeschema, typename);

		free(typeschema);
		free(typename);
	}

	/* print attributes */
	for (i = 0; i < t->nattributes; i++)
	{
		/*
		 * Skip column if it is a typed table because its definition is already
		 * there.
		 */
		if (t->reloftype.oid != InvalidOid)
			continue;

		/* first attribute */
		if (hasatts)
			fprintf(output, ",\n");
		else
			fprintf(output, "(\n");
		hasatts = true;

		/* attribute name and type */
		fprintf(output, "%s %s", t->attributes[i].attname, t->attributes[i].atttypname);

		/* collate */
		/* XXX schema-qualified? */
		if (t->attributes[i].attcollation != NULL)
			fprintf(output, " COLLATE \"%s\"", t->attributes[i].attcollation);

		/* default value? */
		if (t->attributes[i].attdefexpr != NULL)
			fprintf(output, " DEFAULT %s", t->attributes[i].attdefexpr);

		/* not null? */
		if (t->attributes[i].attnotnull)
			fprintf(output, " NOT NULL");
	}

	/* print check constraints */
	for (i = 0; i < t->ncheck; i++)
	{
		/* first attribute */
		if (hasatts)
			fprintf(output, ",\n");
		else
			fprintf(output, "(\n");
		hasatts = true;

		fprintf(output, "CONSTRAINT %s %s", t->check[i].conname, t->check[i].condef);
	}

	if (hasatts)
		fprintf(output, "\n)");
	else if (t[i].reloftype.oid == InvalidOid)
		fprintf(output, "(\n)");

	/* partitioned table */
	if (PGQ_IS_PARTITIONED_TABLE(t->kind))
		fprintf(output, "\nPARTITION BY %s", t->partitionkey);

	/* foreign server */
	if (PGQ_IS_FOREIGN_TABLE(t->kind))
		fprintf(output, "\nSERVER %s", t->servername);

	/* reloptions */
	if (t->reloptions != NULL)
		fprintf(output, "\nWITH (%s)", t->reloptions);

	/* foreign table options */
	if (PGQ_IS_FOREIGN_TABLE(t->kind) && t->ftoptions != NULL)
		fprintf(output, "\nOPTIONS (%s)", t->ftoptions);

	fprintf(output, ";");

	/* partition */
	if (t->partition)
		dumpAttachPartition(output2, t);

	/* replica identity */
	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind) &&
			t->relreplident != 'v')		/* 'v' (void) means < 9.4 */
	{

		switch (t->relreplident)
		{
				char	*replident;

			case 'n':
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY NOTHING;", schema,
						tabname);
				break;
			case 'd':
				/* print nothing. After all, it is the default */
				break;
			case 'f':
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY FULL;", schema,
						tabname);
				break;
			case 'i':
				replident = formatObjectIdentifier(t->relreplidentidx);
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY USING INDEX %s;",
						schema, tabname, replident);
				free(replident);
				break;
			default:
				logWarning("replica identity %c is invalid", t->relreplident);
		}
	}

	/* statistics target */
	for (i = 0; i < t->nattributes; i++)
	{
		dumpAlterColumnSetStatistics(output, t, i, false);
		dumpAlterColumnSetStorage(output, t, i, false);
	}

	/* print primary key */
	if (t->pk.conname != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
		fprintf(output, "\tADD CONSTRAINT %s %s", t->pk.conname, t->pk.condef);
		fprintf(output, ";");
	}

	/* print foreign key constraints */
	for (i = 0; i < t->nfk; i++)
	{
		fprintf(output2, "\n\n");
		fprintf(output2, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
		fprintf(output2, "\tADD CONSTRAINT %s %s", t->fk[i].conname, t->fk[i].condef);
		fprintf(output2, ";");
	}

	/* XXX Should it belong to sequence.c? */
	/* print owned by sequences */
	for (i = 0; i < t->nownedby; i++)
	{
		char	*seqschema = formatObjectIdentifier(t->seqownedby[i].schemaname);
		char	*seqname = formatObjectIdentifier(t->seqownedby[i].objectname);
		char	*attname = formatObjectIdentifier(t->attownedby[i]);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER SEQUENCE %s.%s OWNED BY %s.%s.%s;", seqschema, seqname,
				schema, tabname, attname);

		free(seqschema);
		free(seqname);
		free(attname);
	}

	/* comment */
	if (options.comment)
	{
		if (t->comment != NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON %s %s.%s IS %s;", kind, schema, tabname,
					t->comment);
		}

		/* columns */
		for (i = 0; i < t->nattributes; i++)
		{
			if (t->attributes[i].comment != NULL)
			{
				char	*attname = formatObjectIdentifier(t->attributes[i].attname);

				fprintf(output, "\n\n");
				fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS %s;", schema, tabname, attname,
						t->attributes[i].comment);

				free(attname);
			}
		}

		/* primary key */
		if (t->pk.conname != NULL && t->pk.comment != NULL)
		{
			char	*pkname = formatObjectIdentifier(t->pk.conname);

			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS %s;", pkname, schema,
					tabname, t->pk.comment);

			free(pkname);
		}

		/* foreign key */
		for (i = 0; i < t->nfk; i++)
		{
			if (t->fk[i].comment != NULL)
			{
				char	*fkname = formatObjectIdentifier(t->fk[i].conname);

				fprintf(output, "\n\n");
				fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS %s;", fkname, schema,
						tabname, t->fk[i].comment);

				free(fkname);
			}
		}

		/* check constraint */
		for (i = 0; i < t->ncheck; i++)
		{
			if (t->check[i].comment != NULL)
			{
				char	*ckname = formatObjectIdentifier(t->check[i].conname);

				fprintf(output, "\n\n");
				fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS %s;", ckname, schema,
						tabname, t->check[i].comment);

				free(ckname);
			}
		}
	}

	/* attribute options */
	for (i = 0; i < t->nattributes; i++)
	{
		if (t->attributes[i].attoptions)
		{
			char	*attname = formatObjectIdentifier(t->attributes[i].attname);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET (%s)", kind, schema,
					tabname, attname, t->attributes[i].attoptions);

			free(attname);
		}
	}

	/* security labels */
	if (options.securitylabels && t->nseclabels > 0)
	{
		for (i = 0; i < t->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS %s;",
					t->seclabels[i].provider,
					kind,
					schema,
					tabname,
					t->seclabels[i].label);
		}

		/* attributes */
		for (i = 0; i < t->nattributes; i++)
		{
			if (t->attributes[i].nseclabels > 0)
			{
				char	*attname = formatObjectIdentifier(t->attributes[i].attname);
				int	j;

				for (j = 0; j < t->attributes[i].nseclabels; j++)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS %s;",
							t->attributes[i].seclabels[j].provider,
							schema,
							tabname,
							attname,
							t->attributes[i].seclabels[j].label);
				}

				free(attname);
			}
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(t->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s %s.%s OWNER TO %s;", kind, schema, tabname, owner);

		free(owner);
	}

	/* privileges */
	/* XXX second t->obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
	{
		dumpGrantAndRevoke(output, PGQ_TABLE, &t->obj, &t->obj, NULL, t->acl, NULL,
						   NULL);

		/* attribute ACL */
		for (i = 0; i < t->nattributes; i++)
		{
			if (t->attributes[i].acl)
			{
				char	*attname = formatObjectIdentifier(t->attributes[i].attname);

				dumpGrantAndRevoke(output, PGQ_TABLE, &t->obj, &t->obj, NULL,
								   t->attributes[i].acl, NULL, attname);

				free(attname);
			}
		}
	}

	free(kind);
	free(schema);
	free(tabname);
}

static void
dumpAddColumn(FILE *output, PQLTable *t, int i)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);
	char	*attname = formatObjectIdentifier(t->attributes[i].attname);
	char	*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	fprintf(output, "\n\n");
	fprintf(output, "ALTER %s ONLY %s.%s ADD COLUMN %s %s", kind, schema, tabname,
			attname, t->attributes[i].atttypname);

	/* collate */
	/* XXX schema-qualified? */
	if (t->attributes[i].attcollation != NULL)
		fprintf(output, " COLLATE \"%s\"", t->attributes[i].attcollation);

	/* default value? */
	if (t->attributes[i].attdefexpr != NULL)
		fprintf(output, " DEFAULT %s", t->attributes[i].attdefexpr);

	/* not null? */
	/*
	 * XXX if the table already contains data it will fail miserably unless you
	 * XXX declare the DEFAULT clause. One day, this piece of code will be
	 * XXX smarter to warn the OP that postgres cannot automagically set not
	 * XXX null in a table that already contains data.
	 */
	if (t->attributes[i].attnotnull)
		fprintf(output, " NOT NULL");

	/* attribute options */
	if (t->attributes[i].attoptions)
		fprintf(output, " SET (%s)", t->attributes[i].attoptions);

	fprintf(output, ";");

	/* comment */
	if (options.comment && t->attributes[i].comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS %s;", schema, tabname, attname,
				t->attributes[i].comment);
	}

	/* security labels */
	if (options.securitylabels && t->attributes[i].nseclabels > 0)
	{
		int	j;

		for (j = 0; j < t->attributes[i].nseclabels; j++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS %s;",
					t->attributes[i].seclabels[j].provider,
					schema,
					tabname,
					attname,
					t->attributes[i].seclabels[j].label);
		}
	}

	/* privileges */
	if (options.privileges && t->attributes[i].acl)
		dumpGrantAndRevoke(output, PGQ_TABLE, &t->obj, &t->obj, NULL,
						   t->attributes[i].acl, NULL, attname);

	free(kind);
	free(schema);
	free(tabname);
	free(attname);
}

static void
dumpRemoveColumn(FILE *output, PQLTable *t, int i)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);
	char	*attname = formatObjectIdentifier(t->attributes[i].attname);
	char	*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	fprintf(output, "\n\n");
	fprintf(output, "ALTER %s ONLY %s.%s DROP COLUMN %s;", kind, schema, tabname,
			attname);

	free(kind);
	free(schema);
	free(tabname);
	free(attname);
}

static void
dumpAlterColumn(FILE *output, PQLTable *a, int i, PQLTable *b, int j)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*tabname1 = formatObjectIdentifier(a->obj.objectname);
	char	*attname1 = formatObjectIdentifier(a->attributes[i].attname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*tabname2 = formatObjectIdentifier(b->obj.objectname);
	char	*attname2 = formatObjectIdentifier(b->attributes[j].attname);
	char	*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(b->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(b->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	/*
	 * Although we emit a command to change the type of the column of a table,
	 * we do not guarantee that that command will succeed. A USING clause must
	 * be provided to convert from old data type to new. If it is omitted, the
	 * default conversion is the same as an assignment cast from old data type
	 * to new (that is the case right now).
	 *
	 * FIXME Provide a USING clause for those cases that there is no implicit
	 * or assignment cast.
	 * XXX It means that this command could fail miserably to apply.
	 */
	if (strcmp(a->attributes[i].atttypname, b->attributes[j].atttypname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET DATA TYPE %s",
				kind, schema2, tabname2, attname2, b->attributes[j].atttypname);

		/* collate */
		/* XXX schema-qualified? */
		if (b->attributes[j].attcollation != NULL)
			fprintf(output, " COLLATE \"%s\"", b->attributes[j].attcollation);

		fprintf(output, ";");
	}

	/* default value? */
	if (a->attributes[i].attdefexpr == NULL && b->attributes[j].attdefexpr != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET DEFAULT %s;",
				kind, schema2, tabname2, attname2, b->attributes[j].attdefexpr);
	}
	else if (a->attributes[i].attdefexpr != NULL &&
			 b->attributes[j].attdefexpr == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s DROP DEFAULT;", kind,
				schema2,
				tabname2, attname2);
	}

	/* not null? */
	if (!a->attributes[i].attnotnull && b->attributes[j].attnotnull)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET NOT NULL;", kind,
				schema2,
				tabname2, attname2);
	}
	else if (a->attributes[i].attnotnull && !b->attributes[j].attnotnull)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s DROP NOT NULL;",
				kind, schema2, tabname2, attname2);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->attributes[i].comment == NULL && b->attributes[j].comment != NULL) ||
				(a->attributes[i].comment != NULL && b->attributes[j].comment != NULL &&
				 strcmp(a->attributes[i].comment, b->attributes[j].comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS %s;", schema2, tabname2,
					attname2, b->attributes[j].comment);
		}
		else if (a->attributes[i].comment != NULL && b->attributes[j].comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON COLUMN %s.%s.%s IS NULL;", schema2, tabname2,
					attname2);
		}
	}

	/* security labels */
	if (options.securitylabels)
	{
		if (a->attributes[i].seclabels == NULL && b->attributes[j].seclabels != NULL)
		{
			int	k;

			for (k = 0; k < b->attributes[j].nseclabels; k++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS %s;",
						b->attributes[j].seclabels[k].provider,
						schema2,
						tabname2,
						attname2,
						b->attributes[j].seclabels[k].label);
			}
		}
		else if (a->attributes[i].seclabels != NULL &&
				 b->attributes[j].seclabels == NULL)
		{
			int	k;

			for (k = 0; k < a->nseclabels; k++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS NULL;",
						a->attributes[i].seclabels[k].provider,
						schema1,
						tabname1,
						attname1);
			}
		}
		else if (a->attributes[i].seclabels != NULL &&
				 b->attributes[j].seclabels != NULL)
		{
			int	k, l;

			k = l = 0;
			while (k < a->attributes[i].nseclabels || l < b->attributes[j].nseclabels)
			{
				if (k == a->attributes[i].nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS %s;",
							b->attributes[j].seclabels[l].provider,
							schema2,
							tabname2,
							attname2,
							b->attributes[j].seclabels[l].label);
					l++;
				}
				else if (l == b->attributes[j].nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS NULL;",
							a->attributes[i].seclabels[k].provider,
							schema1,
							tabname1,
							attname1);
					k++;
				}
				else if (strcmp(a->attributes[i].seclabels[k].provider,
								b->attributes[j].seclabels[l].provider) == 0)
				{
					if (strcmp(a->attributes[i].seclabels[k].label,
							   b->attributes[j].seclabels[l].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS %s;",
								b->attributes[j].seclabels[l].provider,
								schema2,
								tabname2,
								attname2,
								b->attributes[j].seclabels[l].label);
					}
					k++;
					l++;
				}
				else if (strcmp(a->attributes[i].seclabels[k].provider,
								b->attributes[j].seclabels[l].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS NULL;",
							a->attributes[i].seclabels[k].provider,
							schema1,
							tabname1,
							attname1);
					k++;
				}
				else if (strcmp(a->attributes[i].seclabels[k].provider,
								b->attributes[j].seclabels[l].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON COLUMN %s.%s.%s IS %s;",
							b->attributes[j].seclabels[l].provider,
							schema2,
							tabname2,
							attname2,
							b->attributes[j].seclabels[l].label);
					l++;
				}
			}
		}
	}

	/* privileges */
	if (options.privileges)
		dumpGrantAndRevoke(output, PGQ_TABLE, &a->obj, &b->obj, a->attributes[i].acl,
						   b->attributes[j].acl, NULL, attname1);

	free(kind);
	free(schema1);
	free(tabname1);
	free(attname1);
	free(schema2);
	free(tabname2);
	free(attname2);
}

static void
dumpAlterColumnSetStatistics(FILE *output, PQLTable *t, int i, bool force)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);
	char	*attname = formatObjectIdentifier(t->attributes[i].attname);
	char	*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	if (t->attributes[i].attstattarget != -1 || force)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET STATISTICS %d;",
				kind, schema, tabname, attname, t->attributes[i].attstattarget);
	}

	free(kind);
	free(schema);
	free(tabname);
	free(attname);
}

static void
dumpAlterColumnSetStorage(FILE *output, PQLTable *t, int i, bool force)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);
	char	*attname = formatObjectIdentifier(t->attributes[i].attname);
	char	*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(t->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(t->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	if (!t->attributes[i].defstorage || force)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET STORAGE %s;",
				kind, schema, tabname, attname, t->attributes[i].attstorage);
	}

	free(kind);
	free(schema);
	free(tabname);
	free(attname);
}

/*
 * Set attribute options if needed
 */
static void
dumpAlterColumnSetOptions(FILE *output, PQLTable *a, int i, PQLTable *b, int j)
{
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*tabname2 = formatObjectIdentifier(b->obj.objectname);
	char	*attname2 = formatObjectIdentifier(b->attributes[j].attname);
	char	*kind;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(b->kind))
		kind = strdup("TABLE");
	else if (PGQ_IS_FOREIGN_TABLE(b->kind))
		kind = strdup("FOREIGN TABLE");
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	if (a->attributes[i].attoptions == NULL && b->attributes[j].attoptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET (%s);", kind,
				schema2, tabname2, attname2, b->attributes[j].attoptions);
	}
	else if (a->attributes[i].attoptions != NULL &&
			 b->attributes[j].attoptions == NULL)
	{
		stringList	*rlist;

		/* reset all options */
		rlist = setOperationOptions(a->attributes[i].attoptions,
									b->attributes[j].attoptions,
									PGQ_SETDIFFERENCE, false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s RESET (%s);", kind,
					schema2, tabname2, attname2, resetlist);

			free(resetlist);
			freeStringList(rlist);
		}
	}
	else if (a->attributes[i].attoptions != NULL &&
			 b->attributes[j].attoptions != NULL &&
			 strcmp(a->attributes[i].attoptions, b->attributes[j].attoptions) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		/* reset options that are only presented in the first set */
		rlist = setOperationOptions(a->attributes[i].attoptions,
									b->attributes[j].attoptions,
									PGQ_SETDIFFERENCE, false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s RESET (%s);", kind,
					schema2, tabname2, attname2, resetlist);

			free(resetlist);
			freeStringList(rlist);
		}

		/*
		 * Include intersection between option sets. However, exclude
		 * options that don't change.
		 */
		ilist = setOperationOptions(a->attributes[i].attoptions,
									b->attributes[j].attoptions, PGQ_INTERSECT, true, true);
		if (ilist)
		{
			char	*setlist;

			setlist = printOptions(ilist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET (%s);", kind,
					schema2, tabname2, attname2, setlist);

			free(setlist);
			freeStringList(ilist);
		}

		/*
		 * Set options that are only presented in the second set.
		 */
		slist = setOperationOptions(b->attributes[j].attoptions,
									a->attributes[i].attoptions, PGQ_SETDIFFERENCE, true, true);
		if (slist)
		{
			char	*setlist;

			setlist = printOptions(slist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s ONLY %s.%s ALTER COLUMN %s SET (%s);", kind,
					schema2, tabname2, attname2, setlist);

			free(setlist);
			freeStringList(slist);
		}
	}

	free(kind);
	free(schema2);
	free(tabname2);
	free(attname2);
}

static void
dumpAddPK(FILE *output, PQLTable *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
	fprintf(output, "\tADD CONSTRAINT %s %s", t->pk.conname, t->pk.condef);
	fprintf(output, ";");

	if (options.comment)
	{
		if (t->pk.comment != NULL)
		{
			char	*pkname = formatObjectIdentifier(t->pk.conname);

			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS %s;", pkname, schema,
					tabname, t->pk.comment);

			free(pkname);
		}
	}

	free(schema);
	free(tabname);
}

static void
dumpRemovePK(FILE *output, PQLTable *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
	fprintf(output, "\tDROP CONSTRAINT %s", t->pk.conname);
	fprintf(output, ";");

	free(schema);
	free(tabname);
}

static void
dumpAddFK(FILE *output, PQLTable *t, int i)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
	fprintf(output, "\tADD CONSTRAINT %s %s", t->fk[i].conname, t->fk[i].condef);
	fprintf(output, ";");

	if (options.comment)
	{
		if (t->fk[i].comment != NULL)
		{
			char	*fkname = formatObjectIdentifier(t->fk[i].conname);

			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS %s;", fkname, schema,
					tabname, t->fk[i].comment);

			free(fkname);
		}
	}

	free(schema);
	free(tabname);
}

static void
dumpRemoveFK(FILE *output, PQLTable *t, int i)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
	fprintf(output, "\tDROP CONSTRAINT %s", t->fk[i].conname);
	fprintf(output, ";");

	free(schema);
	free(tabname);
}

static void
dumpAddCheck(FILE *output, PQLTable *t, int i)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
	fprintf(output, "\tADD CONSTRAINT %s %s", t->check[i].conname, t->check[i].condef);
	fprintf(output, ";");

	if (options.comment)
	{
		if (t->check[i].comment != NULL)
		{
			char	*checkname = formatObjectIdentifier(t->check[i].conname);

			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON CONSTRAINT %s ON %s.%s IS %s;", checkname, schema,
					tabname, t->check[i].comment);

			free(checkname);
		}
	}

	free(schema);
	free(tabname);
}

static void
dumpRemoveCheck(FILE *output, PQLTable *t, int i)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tabname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE ONLY %s.%s\n", schema, tabname);
	fprintf(output, "\tDROP CONSTRAINT %s", t->check[i].conname);
	fprintf(output, ";");

	free(schema);
	free(tabname);
}

static void
dumpAttachPartition(FILE *output, PQLTable *a)
{
	char	*schema = formatObjectIdentifier(a->obj.schemaname);
	char	*tabname = formatObjectIdentifier(a->obj.objectname);
	char	*parentschema = formatObjectIdentifier(a->parent[0].schemaname);
	char	*parentname = formatObjectIdentifier(a->parent[0].objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE %s.%s ATTACH PARTITION %s.%s %s;", parentschema,
			parentname, schema, tabname, a->partitionbound);

	free(schema);
	free(tabname);
	free(parentschema);
	free(parentname);
}

static void
dumpDetachPartition(FILE *output, PQLTable *a)
{
	char	*schema = formatObjectIdentifier(a->obj.schemaname);
	char	*tabname = formatObjectIdentifier(a->obj.objectname);
	char	*parentschema = formatObjectIdentifier(a->parent[0].schemaname);
	char	*parentname = formatObjectIdentifier(a->parent[0].objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER TABLE %s.%s DETACH PARTITION %s.%s;", parentschema,
			parentname, schema, tabname);

	free(schema);
	free(tabname);
	free(parentschema);
	free(parentname);
}

void
dumpAlterTable(FILE *output, PQLTable *a, PQLTable *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*tabname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*tabname2 = formatObjectIdentifier(b->obj.objectname);
	char	*kind, *kindl;
	int		i, j;

	if (PGQ_IS_REGULAR_OR_PARTITIONED_TABLE(b->kind))
	{
		kind = strdup("TABLE");
		kindl = strdup("table");
	}
	else if (PGQ_IS_FOREIGN_TABLE(b->kind))
	{
		kind = strdup("FOREIGN TABLE");
		kindl = strdup("foreign table");
	}
	else
	{
		logError("table is not regular, partitioned or foreign");
		exit(EXIT_FAILURE);
	}

	/* regular or foreign table */
	if (a->reloftype.oid == InvalidOid && b->reloftype.oid == InvalidOid)
	{
		/* the attributes are sorted by name */
		i = j = 0;
		while (i < a->nattributes || j < b->nattributes)
		{
			/*
			 * End of table a attributes. Additional columns from table b will be
			 * added.
			 */
			if (i == a->nattributes)
			{
				logDebug("%s \"%s\".\"%s\" attribute \"%s\" (%s) added",
						 kindl, b->obj.schemaname, b->obj.objectname,
						 b->attributes[j].attname, b->attributes[j].atttypname);

				dumpAddColumn(output, b, j);

				dumpAlterColumnSetStatistics(output, b, j, false);	/* statistics target */
				dumpAlterColumnSetStorage(output, b, j, false);		/* storage */

				j++;
			}
			/*
			 * End of table b attributes. Additional columns from table a will be
			 * removed.
			 */
			else if (j == b->nattributes)
			{
				logDebug("%s \"%s\".\"%s\" attribute \"%s\" (%s) removed", kindl,
						 a->obj.schemaname,
						 a->obj.objectname, a->attributes[i].attname, a->attributes[i].atttypname);

				dumpRemoveColumn(output, a, i);
				i++;
			}
			else if (strcmp(a->attributes[i].attname, b->attributes[j].attname) == 0)
			{
				/*
				 * Same column name but check other attribute definition
				 *   - data types
				 *   - default value
				 *   - not null
				 *   - comment
				 *   - security labels
				 *   - privileges
				 */
				dumpAlterColumn(output, a, i, b, j);

				/* do attribute options change? */
				dumpAlterColumnSetOptions(output, a, i, b, j);

				/* column statistics changed */
				if (a->attributes[i].attstattarget != b->attributes[j].attstattarget)
					dumpAlterColumnSetStatistics(output, b, j, true);

				/* storage changed */
				if (a->attributes[i].defstorage != b->attributes[j].defstorage)
					dumpAlterColumnSetStorage(output, b, j, true);

				/* attribute ACL changed */
				if (options.privileges)
				{
					char *attname = formatObjectIdentifier(a->attributes[i].attname);

					if (a->attributes[i].acl != NULL && b->attributes[j].acl == NULL)
						dumpGrantAndRevoke(output, PGQ_TABLE, &a->obj, &b->obj, a->attributes[i].acl,
										   NULL, NULL, attname);
					else if (a->attributes[i].acl == NULL && b->attributes[j].acl != NULL)
						dumpGrantAndRevoke(output, PGQ_TABLE, &a->obj, &b->obj, NULL,
										   b->attributes[j].acl, NULL, attname);
					else if (a->attributes[i].acl != NULL && b->attributes[j].acl != NULL &&
							 strcmp(a->attributes[i].acl, b->attributes[j].acl) != 0)
						dumpGrantAndRevoke(output, PGQ_TABLE, &a->obj, &b->obj, a->attributes[i].acl,
										   b->attributes[j].acl, NULL, attname);

					free(attname);
				}

				i++;
				j++;
			}
			else if (strcmp(a->attributes[i].attname, b->attributes[j].attname) < 0)
			{
				logDebug("%s \"%s\".\"%s\" attribute \"%s\" (%s) removed",
						 kindl, a->obj.schemaname, a->obj.objectname,
						 a->attributes[i].attname, a->attributes[i].atttypname);

				dumpRemoveColumn(output, a, i);
				i++;
			}
			else if (strcmp(a->attributes[i].attname, b->attributes[j].attname) > 0)
			{
				logDebug("%s \"%s\".\"%s\" attribute \"%s\" (%s) added",
						 kindl, b->obj.schemaname, b->obj.objectname,
						 b->attributes[j].attname, b->attributes[j].atttypname);

				dumpAddColumn(output, b, j);

				dumpAlterColumnSetStatistics(output, b, j, false);	/* statistics target */
				dumpAlterColumnSetStorage(output, b, j, false);		/* storage */

				j++;
			}
		}

		/* the FKs are sorted by name */
		i = j = 0;
		while (i < a->nfk || j < b->nfk)
		{
			/*
			 * End of table a FKs. Additional FKs from table b will be
			 * added.
			 */
			if (i == a->nfk)
			{
				logDebug("%s \"%s\".\"%s\" FK \"%s\" added",
						 kindl, b->obj.schemaname, b->obj.objectname,
						 b->fk[j].conname);

				dumpAddFK(output, b, j);

				j++;
			}
			/*
			 * End of table b FKs. Additional FKs from table a will be
			 * removed.
			 */
			else if (j == b->nfk)
			{
				logDebug("%s \"%s\".\"%s\" FK \"%s\" removed", kindl, a->obj.schemaname,
						 a->obj.objectname, a->fk[i].conname);

				dumpRemoveFK(output, a, i);
				i++;
			}
			else if (strcmp(a->fk[i].conname, b->fk[j].conname) == 0)
			{
				/* drop and create FK again if the definition does not match */
				if (strcmp(a->fk[i].condef, b->fk[j].condef) != 0)
				{
					logDebug("%s \"%s\".\"%s\" FK \"%s\" altered",
							 kindl, b->obj.schemaname, b->obj.objectname,
							 b->fk[j].conname);

					dumpRemoveFK(output, a, i);
					dumpAddFK(output, b, j);
				}

				i++;
				j++;
			}
			else if (strcmp(a->fk[i].conname, b->fk[j].conname) < 0)
			{
				logDebug("%s \"%s\".\"%s\" FK \"%s\" removed",
						 kindl, a->obj.schemaname, a->obj.objectname,
						 a->fk[i].conname);

				dumpRemoveFK(output, a, i);
				i++;
			}
			else if (strcmp(a->fk[i].conname, b->fk[j].conname) > 0)
			{
				logDebug("%s \"%s\".\"%s\" FK \"%s\" added",
						 kindl, b->obj.schemaname, b->obj.objectname,
						 b->fk[j].conname);

				dumpAddFK(output, b, j);

				j++;
			}
		}

		/* the checks are sorted by name */
		i = j = 0;
		while (i < a->ncheck || j < b->ncheck)
		{
			/*
			 * End of table a checks. Additional checks from table b will be
			 * added.
			 */
			if (i == a->ncheck)
			{
				logDebug("%s \"%s\".\"%s\" check \"%s\" added",
						 kindl, b->obj.schemaname, b->obj.objectname,
						 b->check[j].conname);

				dumpAddCheck(output, b, j);

				j++;
			}
			/*
			 * End of table b checks. Additional checks from table a will be
			 * removed.
			 */
			else if (j == b->ncheck)
			{
				logDebug("%s \"%s\".\"%s\" check \"%s\" removed", kindl, a->obj.schemaname,
						 a->obj.objectname, a->check[i].conname);

				dumpRemoveCheck(output, a, i);
				i++;
			}
			else if (strcmp(a->check[i].conname, b->check[j].conname) == 0)
			{
				/* drop and create check again if the definition does not match */
				if (strcmp(a->check[i].condef, b->check[j].condef) != 0)
				{
					logDebug("%s \"%s\".\"%s\" check \"%s\" altered",
							 kindl, b->obj.schemaname, b->obj.objectname,
							 b->check[j].conname);

					dumpRemoveCheck(output, a, i);
					dumpAddCheck(output, b, j);
				}

				i++;
				j++;
			}
			else if (strcmp(a->check[i].conname, b->check[j].conname) < 0)
			{
				logDebug("%s \"%s\".\"%s\" check \"%s\" removed",
						 kindl, a->obj.schemaname, a->obj.objectname,
						 a->check[i].conname);

				dumpRemoveCheck(output, a, i);
				i++;
			}
			else if (strcmp(a->check[i].conname, b->check[j].conname) > 0)
			{
				logDebug("%s \"%s\".\"%s\" check \"%s\" added",
						 kindl, b->obj.schemaname, b->obj.objectname,
						 b->check[j].conname);

				dumpAddCheck(output, b, j);

				j++;
			}
		}

		/* primary key */
		if (a->pk.conname == NULL && b->pk.conname != NULL)
		{
			logDebug("%s \"%s\".\"%s\" PK \"%s\" added",
					 kindl, b->obj.schemaname, b->obj.objectname,
					 b->pk.conname);

			dumpAddPK(output, b);
		}
		else if (a->pk.conname != NULL && b->pk.conname == NULL)
		{
			logDebug("%s \"%s\".\"%s\" PK \"%s\" removed",
					 kindl, a->obj.schemaname, a->obj.objectname,
					 a->pk.conname);

			dumpRemovePK(output, a);
		}
		else if (a->pk.conname != NULL && b->pk.conname != NULL &&
				 strcmp(a->pk.condef, b->pk.condef) != 0)
		{
			logDebug("%s \"%s\".\"%s\" PK \"%s\" altered",
					 kindl, b->obj.schemaname, b->obj.objectname,
					 b->pk.conname);

			dumpRemovePK(output, a);
			dumpAddPK(output, b);
		}
	}
	else
	{
		/* typed table */
		if (a->reloftype.oid == InvalidOid && b->reloftype.oid != InvalidOid)
		{
			char	*typeschema = formatObjectIdentifier(b->reloftype.schemaname);
			char	*typename = formatObjectIdentifier(b->reloftype.objectname);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE ONLY %s.%s OF %s.%s;", schema2, tabname2,
					typeschema, typename);

			free(typeschema);
			free(typename);
		}
		else if (a->reloftype.oid != InvalidOid && b->reloftype.oid == InvalidOid)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE ONLY %s.%s NOT OF;", schema2, tabname2);
		}
		else
		{
			/* TODO check if it is safe to change the type of a typed table */
			logWarning("typed table %s.%s changed its type", schema2, tabname2);
		}
	}

	/* partitioned table cannot be converted to regular table and vice-versa */
	if (PGQ_IS_REGULAR_TABLE(a->kind) && PGQ_IS_PARTITIONED_TABLE(b->kind))
		logWarning("regular table %s.%s cannot be converted to partitioned table",
				   schema1, tabname1);
	else if (PGQ_IS_PARTITIONED_TABLE(a->kind) && PGQ_IS_REGULAR_TABLE(b->kind))
		logWarning("partitioned table %s.%s cannot be converted to regular table",
				   schema1, tabname1);

	/* partition */
	if (!a->partition && b->partition)
		dumpAttachPartition(output, b);
	else if (a->partition && !b->partition)
		dumpDetachPartition(output, a);

	/* reloptions */
	if (a->reloptions == NULL && b->reloptions != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER %s %s.%s SET (%s);", kind, schema2, tabname2,
				b->reloptions);
	}
	else if (a->reloptions != NULL && b->reloptions == NULL)
	{
		stringList	*rlist;

		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE,
									false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s %s.%s RESET (%s);", kind, schema2, tabname2,
					resetlist);

			free(resetlist);
			freeStringList(rlist);
		}
	}
	else if (a->reloptions != NULL && b->reloptions != NULL &&
			 strcmp(a->reloptions, b->reloptions) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		rlist = setOperationOptions(a->reloptions, b->reloptions, PGQ_SETDIFFERENCE,
									false, true);
		if (rlist)
		{
			char	*resetlist;

			resetlist = printOptions(rlist);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s %s.%s RESET (%s);", kind, schema2, tabname2,
					resetlist);

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
			fprintf(output, "ALTER %s %s.%s SET (%s);", kind, schema2, tabname2, setlist);

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
			fprintf(output, "ALTER %s %s.%s SET (%s);", kind, schema2, tabname2, setlist);

			free(setlist);
			freeStringList(slist);
		}
	}

	/*
	 * replica identity
	 *
	 * This feature is only emitted iif both servers support REPLICA IDENTITY.
	 * Otherwise, users will be warned.
	 */
	if (a->relreplident != 'v' && b->relreplident != 'v')
	{
		if (a->relreplident != b->relreplident)
		{
			switch (b->relreplident)
			{
					char	*replident;
				case 'n':
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY NOTHING;", schema2,
							tabname2);
					break;
				case 'd':
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY DEFAULT;", schema2,
							tabname2);
					break;
				case 'f':
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY FULL;", schema2,
							tabname2);
					break;
				case 'i':
					replident = formatObjectIdentifier(b->relreplidentidx);
					fprintf(output, "\n\n");
					fprintf(output, "ALTER TABLE ONLY %s.%s REPLICA IDENTITY USING INDEX %s;",
							schema2, tabname2, replident);
					free(replident);
					break;
				default:
					logWarning("replica identity %c is invalid", b->relreplident);
			}
		}
	}
	else if (PGQ_IS_FOREIGN_TABLE(b->kind))
	{
		/* foreign tables doesn't have REPLICA IDENTITY */
		;
	}
	else
		logWarning("ignoring replica identity because some server does not support it");

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON %s %s.%s IS %s;", kind, schema2, tabname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON %s %s.%s IS NULL;", kind, schema2, tabname2);
		}
	}

	/* security labels */
	if (options.securitylabels)
	{
		if (a->seclabels == NULL && b->seclabels != NULL)
		{
			for (i = 0; i < b->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS %s;",
						b->seclabels[i].provider,
						kind,
						schema2,
						tabname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS NULL;",
						a->seclabels[i].provider,
						kind,
						schema1,
						tabname1);
			}
		}
		else if (a->seclabels != NULL && b->seclabels != NULL)
		{
			i = j = 0;
			while (i < a->nseclabels || j < b->nseclabels)
			{
				if (i == a->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS %s;",
							b->seclabels[j].provider,
							kind,
							schema2,
							tabname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS NULL;",
							a->seclabels[i].provider,
							kind,
							schema1,
							tabname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS %s;",
								b->seclabels[j].provider,
								kind,
								schema2,
								tabname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS NULL;",
							a->seclabels[i].provider,
							kind,
							schema1,
							tabname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON %s %s.%s IS %s;",
							b->seclabels[j].provider,
							kind,
							schema2,
							tabname2,
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
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER %s %s.%s OWNER TO %s;", kind, schema2, tabname2, owner);

			free(owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_TABLE, &a->obj, &b->obj, a->acl, b->acl, NULL,
							   NULL);
	}

	free(kind);
	free(kindl);
	free(schema1);
	free(tabname1);
	free(schema2);
	free(tabname2);
}
