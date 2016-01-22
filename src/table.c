#include "table.h"

/*
 * CREATE TABLE
 * DROP TABLE
 * ALTER TABLE
 * ALTER TABLE ... ALTER COLUMN ... SET STATISTICS
 * ALTER TABLE ... ALTER COLUMN ... SET STORAGE
 * COMMENT ON TABLE
 * COMMENT ON COLUMN
 * COMMENT ON CONSTRAINT
 *
 * TODO
 *
 * CREATE TABLE ... INHERITS
 * CREATE TABLE ... OF type_name
 *
 * ALTER TABLE ... RENAME CONSTRAINT ... TO
 * ALTER TABLE ... RENAME TO
 * ALTER TABLE ... SET SCHEMA TO
 * ALTER TABLE ... SET (...)
 * ALTER TABLE ... RESET (...)
 * ALTER TABLE ... VALIDATE CONSTRAINT
 * ALTER TABLE ... DROP CONSTRAINT
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

enum PQLSetOperation
{
	PGQ_INTERSECT = 0,
	PGQ_EXCEPT = 1
};

static void dumpAddColumn(FILE *output, PQLTable t, int i);
static void dumpRemoveColumn(FILE *output, PQLTable t, int i);
static void dumpAlterColumn(FILE *output, PQLTable a, int i, PQLTable b, int j);
static void dumpAlterColumnSetStatistics(FILE *output, PQLTable a, int i, bool force);
static void dumpAlterColumnSetStorage(FILE *output, PQLTable a, int i, bool force);
static stringList *buildRelOptions(char *options);
static stringListCell *intersectWithSortedLists(stringListCell *a, stringListCell *b);
static stringListCell *exceptWithSortedLists(stringListCell *a, stringListCell *b);
static char *diffRelOptions(char *a, char *b, int kind);

PQLTable *
getTables(PGconn *c, int *n)
{
	PQLTable	*t;
	PGresult	*res;
	int			i;

	logNoise("table: server version: %d", PQserverVersion(c));

	/* FIXME relpersistence (9.1)? */
	if (PQserverVersion(c) >= 90100)
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, c.relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'r' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");
	else
		res = PQexec(c,
					 "SELECT c.oid, n.nspname, c.relname, t.spcname AS tablespacename, 'p' AS relpersistence, array_to_string(c.reloptions, ', ') AS reloptions, obj_description(c.oid, 'pg_class') AS description, pg_get_userbyid(c.relowner) AS relowner, relacl FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) LEFT JOIN pg_tablespace t ON (c.reltablespace = t.oid) WHERE relkind = 'r' AND nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname, relname");

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
		t[i].nattributes = 0;
		t[i].pk.conname = NULL;
		t[i].pk.condef = NULL;
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
		do {
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
				t[i].check[j].comment = strdup(PQgetvalue(res, j, PQfnumber(res, "description")));
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
		do {
			query = (char *) malloc(nquery * sizeof(char));

			r = snprintf(query, nquery,
					"SELECT conname, pg_get_constraintdef(c.oid) AS condef, d.description FROM pg_constraint c LEFT JOIN (pg_description d INNER JOIN pg_class x ON (x.oid = d.classoid AND x.relname = 'pg_constraint')) ON (d.objoid = c.oid) WHERE conrelid = %u AND contype = 'f' ORDER BY conname",
					t[i].obj.oid);

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
			t[i].fk[j].comment = strdup(PQgetvalue(res, j, PQfnumber(res, "description")));
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
		do {
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

		if (PQntuples(res) == 1)
		{
			t[i].pk.conname = strdup(PQgetvalue(res, 0, PQfnumber(res, "conname")));
			t[i].pk.condef = strdup(PQgetvalue(res, 0, PQfnumber(res, "condef")));
			t[i].pk.comment = strdup(PQgetvalue(res, 0, PQfnumber(res, "description")));
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

	do {
		query = (char *) malloc(nquery * sizeof(char));

		/* FIXME attcollation (9.1)? */
		r = snprintf(query, nquery,
			"SELECT a.attnum, a.attname, a.attnotnull, pg_catalog.format_type(t.oid, a.atttypmod) as atttypname, pg_get_expr(d.adbin, a.attrelid) as attdefexpr, CASE WHEN a.attcollation <> t.typcollation THEN c.collname ELSE NULL END AS attcollation, s.description, a.attstattarget, a.attstorage, CASE WHEN t.typstorage <> a.attstorage THEN FALSE ELSE TRUE END AS defstorage FROM pg_attribute a LEFT JOIN pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum) LEFT JOIN pg_collation c ON (a.attcollation = c.oid) LEFT JOIN (pg_description s INNER JOIN pg_class x ON (x.oid = s.classoid AND x.relname = 'pg_attribute')) ON (s.objoid = c.oid) WHERE a.attrelid = %u AND a.attnum > 0 AND attisdropped IS FALSE ORDER BY a.attname",
			t->obj.oid);

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
		t->attributes[i].attstattarget = atoi(PQgetvalue(res, i, PQfnumber(res, "attstattarget")));

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
		t->attributes[i].defstorage = (PQgetvalue(res, i, PQfnumber(res, "defstorage"))[0] == 't');

		/* collation */
		if (PQgetisnull(res, i, PQfnumber(res, "attcollation")))
			t->attributes[i].attcollation = NULL;
		else
			t->attributes[i].attcollation = strdup(PQgetvalue(res, i, PQfnumber(res,
												   "attcollation")));

		/* comment */
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t->attributes[i].comment = NULL;
		else
			t->attributes[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

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
}

void
getOwnedBySequences(PGconn *c, PQLTable *t)
{
	char		*query = NULL;
	int			nquery = PGQQRYLEN;
	PGresult	*res;
	int			i;
	int			r;

	do {
		query = (char *) malloc(nquery * sizeof(char));

		r = snprintf(query, nquery,
				"SELECT n.nspname, c.relname, a.attname FROM pg_depend d INNER JOIN pg_class c ON (c.oid = d.objid) INNER JOIN pg_namespace n ON (n.oid = c.relnamespace) INNER JOIN pg_attribute a ON (d.refobjid = a.attrelid AND d.refobjsubid = a.attnum) WHERE d.classid = 'pg_class'::regclass AND d.objsubid = 0 AND d.refobjid = %u AND d.refobjsubid != 0 AND d.deptype = 'a' AND c.relkind = 'S'",
				t->obj.oid);

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
		t->seqownedby[i].schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t->seqownedby[i].objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "relname")));
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
		char	*resetlist;
		char	*setlist;

		resetlist = diffRelOptions(a.reloptions, b.reloptions, PGQ_EXCEPT);
		if (resetlist)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE %s.%s RESET (%s)",
						formatObjectIdentifier(b.obj.schemaname),
						formatObjectIdentifier(b.obj.objectname),
						resetlist);
			fprintf(output, ";");
			free(resetlist);
		}

		setlist = diffRelOptions(b.reloptions, a.reloptions, PGQ_INTERSECT);
		if (setlist)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE %s.%s SET (%s)",
						formatObjectIdentifier(b.obj.schemaname),
						formatObjectIdentifier(b.obj.objectname),
						setlist);
			fprintf(output, ";");
			free(setlist);
		}
	}
	else if (a.reloptions != NULL && b.reloptions == NULL)
	{
		char	*resetlist;

		resetlist = diffRelOptions(a.reloptions, b.reloptions, PGQ_EXCEPT);
		if (resetlist)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER TABLE %s.%s RESET (%s)",
						formatObjectIdentifier(b.obj.schemaname),
						formatObjectIdentifier(b.obj.objectname),
						resetlist);
			fprintf(output, ";");
			free(resetlist);
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

/*
 * Build an ordered linked list from a comma-separated string. If there is no
 * reloptions return NULL.
 */
static stringList *
buildRelOptions(char *options)
{
	stringList		*sl;
	stringListCell	*x;
	char			*tmp;
	char			*p;

	char			*item;
	char			*nextitem;
	int				len;

	sl = (stringList *) malloc(sizeof(stringList));
	sl->head = sl->tail = NULL;

	/* no options, bail out */
	if (options == NULL)
	{
		logDebug("reloptions is empty");
		return sl;
	}

	len = strlen(options);

	tmp = (char *) malloc((len + 1) * sizeof(char));
	tmp = strdup(options);
	p = tmp;

	for (item = p; item; item = nextitem)
	{
		stringListCell	*sc;

		nextitem = strchr(item, ',');
		if (nextitem)
			*nextitem++ = '\0';

		/* left trim */
		while (isspace(*item))
			item++;

		sc = (stringListCell *) malloc(sizeof(stringListCell));
		sc->value = strdup(item);
		sc->next = NULL;

		logNoise("reloption item: \"%s\"", item);

		/* add stringListCell to stringList in order */
		if (sl->tail)
		{
			stringListCell	*cur = sl->head;

			if (strcmp(cur->value, sc->value) > 0)
			{
				sc->next = cur;
				sl->head = sc;
			}
			else
			{
				while (cur != NULL)
				{
					if (cur == sl->tail)
					{
						cur->next = sc;
						sl->tail = sc;
						break;
					}

					if (strcmp(cur->value, sc->value) < 0 && strcmp(cur->next->value, sc->value) >= 0)
					{
						sc->next = cur->next;
						cur->next = sc;
						break;
					}

					cur = cur->next;
				}
			}
		}
		else
		{
			sl->head = sc;
			sl->tail = sc;
		}
	}

	/* check the order */
	for (x = sl->head; x; x = x->next)
		logNoise("reloption in order: \"%s\"", x->value);

	return sl;
}

/*
 * Return a linked list of stringListCell that contains elements from 'a' that
 * is also in 'b'. If 'withvalue' is true, then strings are built with values
 * from 'b' else the list will contain only the options.
 * TODO cleanup tmpa and tmpb memory
 */
static stringListCell *
intersectWithSortedLists(stringListCell *a, stringListCell *b)
{
	stringListCell	*t;
	char			*c, *d;
	char			*tmpa, *tmpb;

	/* end of linked list */
	if (a == NULL || b == NULL)
		return NULL;

	/* both string lists are not NULL */
	tmpa = strdup(a->value);
	tmpb = strdup(b->value);
	c = strtok(tmpa, "=");
	d = strtok(tmpb, "=");

	/*
	 * if same option and value, call recursively incrementing both lists. We
	 * don't want to include reloption that doesn't change the value.
	 * */
	if (strcmp(a->value, b->value) == 0)
		return intersectWithSortedLists(a->next, b->next);

	/* advance "smaller" string list and call recursively */
	if (strcmp(c, d) < 0)
		return intersectWithSortedLists(a->next, b);

	if (strcmp(c, d) > 0)
		return intersectWithSortedLists(a, b->next);

	/*
	 * executed only if both options are equal. If we are here, that is because
	 * the value changed (see comment a few lines above); in this case, we want
	 * to apply this change.
	 * */
	t = (stringListCell *) malloc(sizeof(stringListCell));
	t->value = strdup(a->value);

	/* advance both string lists and call recursively */
	t->next = intersectWithSortedLists(a->next, b->next);

	return t;
}

/*
 * Given two sorted linked lists (A, B), produce another linked list that is
 * the result of 'A minus B' i.e. elements that are only presented in A. If
 * there aren't elements, return NULL.
 * TODO cleanup tmpa and tmpb memory
 */
static stringListCell *
exceptWithSortedLists(stringListCell *a, stringListCell *b)
{
	stringListCell	*t;
	char			*c, *d;
	char			*tmpa, *tmpb;

	/* end of list */
	if (a == NULL)
		return NULL;

	/* latter list is NULL then transverse former list and build a linked list */
	if (b == NULL)
	{
		t = (stringListCell *) malloc(sizeof(stringListCell));
		tmpa = strdup(a->value);
		c = strtok(tmpa, "=");
		t->value = strdup(c);
		t->next = exceptWithSortedLists(a->next, b);

		return t;
	}

	/* both string list are not NULL */
	tmpa = strdup(a->value);
	tmpb = strdup(b->value);
	c = strtok(tmpa, "=");
	d = strtok(tmpb, "=");

	/* advance latter list and call recursively */
	if (strcmp(c, d) > 0)
		return exceptWithSortedLists(a, b->next);

	/* advance both string lists and call recursively */
	if (strcmp(c, d) == 0)
		return exceptWithSortedLists(a->next, b->next);

	/* executed only if A value is not in B */
	t = (stringListCell *) malloc(sizeof(stringListCell));
	t->value = strdup(c);
	t->next = exceptWithSortedLists(a->next, b);

	return t;
}

/*
 * Return an allocated string that contains comma-separated options according
 * to set operation. If there aren't options, return NULL.
 */
static char *
diffRelOptions(char *a, char *b, int kind)
{
	char			*list = NULL;
	stringList		*first, *second;
	stringListCell	*headitem, *p;
	bool			firstitem = true;

	/* if a is NULL, there is neither intersection nor complement (except) */
	if (a == NULL)
		return NULL;

	/* if b is NULL, there isn't intersection */
	if (kind == PGQ_INTERSECT && b == NULL)
		return NULL;

	first = buildRelOptions(a);
	second = buildRelOptions(b);

	if (kind == PGQ_INTERSECT)
		headitem = intersectWithSortedLists(first->head, second->head);
	else if (kind == PGQ_EXCEPT)
		headitem = exceptWithSortedLists(first->head, second->head);
	else
		logError("set operation not supported");

	if (headitem)
	{
		int		listlen;
		int		n = 0;

		/* in the worst case, 'list' will be 'a' */
		listlen = strlen(a) + 1;
		list = (char *) malloc(listlen * sizeof(char));

		/* build a list like 'a=10, b=20, c=30' or 'a, b, c' */
		for (p = headitem; p; p = p->next)
		{
			if (firstitem)
			{
				firstitem = false;
			}
			else
			{
				/* if it is not the first item, add comma and space before it */
				strncpy(list + n, ", ", 2);
				n += 2;
			}

			strcpy(list + n, p->value);
			n += strlen(p->value);
		}
	}

	if (list)
		logNoise("reloptions diff (%d): %s", kind, list);

	if (first)
		freeStringList(first);
	if (second)
		freeStringList(second);

	return list;
}
