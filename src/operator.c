/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * operator.c
 *     Generate OPERATOR [ CLASS | FAMILY] commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE OPERATOR
 * CREATE OPERATOR CLASS
 * CREATE OPERATOR FAMILY
 * DROP OPERATOR
 * DROP OPERATOR CLASS
 * DROP OPERATOR FAMILY
 * ALTER OPERATOR
 * ALTER OPERATOR CLASS
 * ALTER OPERATOR FAMILY
 * COMMENT ON OPERATOR
 * COMMENT ON OPERATOR CLASS
 * COMMENT ON OPERATOR FAMILY
 *
 * TODO
 *
 * ALTER OPERATOR CLASS ... RENAME TO
 * ALTER OPERATOR FAMILY ... RENAME TO
 * ALTER OPERATOR ... SET SCHEMA
 * ALTER OPERATOR CLASS ... SET SCHEMA
 * ALTER OPERATOR FAMILY ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "operator.h"


static void dumpAddOperatorOpFamily(FILE *output, PQLOperatorFamily *f, int i);
static void dumpRemoveOperatorOpFamily(FILE *output, PQLOperatorFamily *f,
									   int i);
static void dumpAddFunctionOpFamily(FILE *output, PQLOperatorFamily *f, int i);
static void dumpRemoveFunctionOpFamily(FILE *output, PQLOperatorFamily *f,
									   int i);

PQLOperator *
getOperators(PGconn *c, int *n)
{
	PQLOperator		*o;
	char			*query;
	PGresult		*res;
	int				i;

	logNoise("operator: server version: %d", PQserverVersion(c));

	query = psprintf("SELECT o.oid, n.nspname, o.oprname, oprcode::regprocedure, oprleft::regtype, oprright::regtype, oprcom::regoperator, oprnegate::regoperator, oprrest::regprocedure, oprjoin::regprocedure, oprcanhash, oprcanmerge, obj_description(o.oid, 'pg_operator') AS description, pg_get_userbyid(o.oprowner) AS oprowner FROM pg_operator o INNER JOIN pg_namespace n ON (o.oprnamespace = n.oid) WHERE o.oid >= %u %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE o.oid = d.objid AND deptype = 'e') ORDER BY n.nspname, o.oprname, o.oprleft, o.oprright", PGQ_FIRST_USER_OID, include_schema_str, exclude_schema_str);

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
		o = (PQLOperator *) malloc(*n * sizeof(PQLOperator));
	else
		o = NULL;

	logDebug("number of operators in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		o[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		o[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		o[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "oprname")));
		o[i].procedure = strdup(PQgetvalue(res, i, PQfnumber(res, "oprcode")));

		if (PQgetisnull(res, i, PQfnumber(res, "oprleft")))
			o[i].lefttype = NULL;
		else
			o[i].lefttype  = strdup(PQgetvalue(res, i, PQfnumber(res, "oprleft")));

		if (PQgetisnull(res, i, PQfnumber(res, "oprright")))
			o[i].righttype = NULL;
		else
			o[i].righttype  = strdup(PQgetvalue(res, i, PQfnumber(res, "oprright")));

		if (strcmp(PQgetvalue(res, i, PQfnumber(res, "oprcom")), "0") == 0)
			o[i].commutator = NULL;
		else
			o[i].commutator  = strdup(PQgetvalue(res, i, PQfnumber(res, "oprcom")));

		if (strcmp(PQgetvalue(res, i, PQfnumber(res, "oprnegate")), "0") == 0)
			o[i].negator = NULL;
		else
			o[i].negator  = strdup(PQgetvalue(res, i, PQfnumber(res, "oprnegate")));

		if (strcmp(PQgetvalue(res, i, PQfnumber(res, "oprrest")), "-") == 0)
			o[i].restriction = NULL;
		else
			o[i].restriction  = strdup(PQgetvalue(res, i, PQfnumber(res, "oprrest")));

		if (strcmp(PQgetvalue(res, i, PQfnumber(res, "oprjoin")), "-") == 0)
			o[i].join = NULL;
		else
			o[i].join  = strdup(PQgetvalue(res, i, PQfnumber(res, "oprjoin")));

		o[i].canhash = (PQgetvalue(res, i, PQfnumber(res, "oprcanhash"))[0] == 't');
		o[i].canmerge = (PQgetvalue(res, i, PQfnumber(res, "oprcanmerge"))[0] == 't');

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			o[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			o[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (o[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		o[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "oprowner")));

		if (o[i].lefttype != NULL && o[i].righttype != NULL)
			logDebug("operator \"%s\".%s(%s, %s)", o[i].obj.schemaname,
					 o[i].obj.objectname, o[i].lefttype, o[i].righttype);
		else if (o[i].lefttype != NULL)
			logDebug("operator \"%s\".%s(%s, NONE)", o[i].obj.schemaname,
					 o[i].obj.objectname, o[i].lefttype);
		else if (o[i].righttype != NULL)
			logDebug("operator \"%s\".%s(NONE, %s)", o[i].obj.schemaname,
					 o[i].obj.objectname, o[i].righttype);
	}

	PQclear(res);

	return o;
}

PQLOperatorClass *
getOperatorClasses(PGconn *c, int *n)
{
	PQLOperatorClass	*d;
	char				*query;
	PGresult			*res;
	int					i;

	logNoise("operator class: server version: %d", PQserverVersion(c));

	query = psprintf("SELECT c.oid, n.nspname AS opcnspname, c.opcname, c.opcdefault, c.opcintype::regtype, a.amname, o.nspname AS opfnspname, f.opfname, CASE WHEN c.opckeytype = 0 THEN NULL ELSE c.opckeytype::regtype END AS storage, obj_description(c.oid, 'pg_opclass') AS description, pg_get_userbyid(c.opcowner) AS opcowner FROM pg_opclass c INNER JOIN pg_namespace n ON (c.opcnamespace = n.oid) INNER JOIN pg_am a ON (c.opcmethod = a.oid) LEFT JOIN (pg_opfamily f INNER JOIN pg_namespace o ON (f.opfnamespace = o.oid)) ON (c.opcfamily = f.oid) WHERE c.oid >= %u %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE c.oid = d.objid AND deptype = 'e') ORDER BY c.opcnamespace, c.opcname", PGQ_FIRST_USER_OID, include_schema_str, exclude_schema_str);

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
		d = (PQLOperatorClass *) malloc(*n * sizeof(PQLOperatorClass));
	else
		d = NULL;

	logDebug("number of operator classes in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		d[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		d[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "opcnspname")));
		d[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "opcname")));
		d[i].defaultopclass = (PQgetvalue(res, i, PQfnumber(res,
										  "opcdefault"))[0] == 't');
		d[i].intype = strdup(PQgetvalue(res, i, PQfnumber(res, "opcintype")));
		d[i].accessmethod = strdup(PQgetvalue(res, i, PQfnumber(res, "amname")));
		if (PQgetisnull(res, i, PQfnumber(res, "opfname")))
		{
			d[i].family.schemaname = NULL;
			d[i].family.objectname = NULL;
		}
		else
		{
			d[i].family.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
											"opfnspname")));
			d[i].family.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "opfname")));
		}
		if (PQgetisnull(res, i, PQfnumber(res, "storage")))
			d[i].storagetype = NULL;
		else
			d[i].storagetype = strdup(PQgetvalue(res, i, PQfnumber(res, "storage")));

		/*
		 * These values are not assigned here (see getOpFuncAttributes), but
		 * default values are essential to avoid having trouble in
		 * freeOperatorClasses.
		 */
		d[i].opandfunc.noperators = 0;
		d[i].opandfunc.operators = NULL;
		d[i].opandfunc.nfunctions = 0;
		d[i].opandfunc.functions = NULL;

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

		d[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "opcowner")));
	}

	PQclear(res);

	return d;
}

PQLOperatorFamily *
getOperatorFamilies(PGconn *c, int *n)
{
	PQLOperatorFamily	*f;
	char				*query;
	PGresult			*res;
	int					i;

	logNoise("operator family: server version: %d", PQserverVersion(c));

	query = psprintf("SELECT f.oid, n.nspname AS opfnspname, f.opfname, a.amname, obj_description(f.oid, 'pg_opfamily') AS description, pg_get_userbyid(f.opfowner) AS opfowner FROM pg_opfamily f INNER JOIN pg_namespace n ON (f.opfnamespace = n.oid) INNER JOIN pg_am a ON (f.opfmethod = a.oid) WHERE f.oid >= %u %s%s AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE f.oid = d.objid AND deptype = 'e') ORDER BY opfnspname, f.opfname", PGQ_FIRST_USER_OID, include_schema_str, exclude_schema_str);

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
		f = (PQLOperatorFamily *) malloc(*n * sizeof(PQLOperatorFamily));
	else
		f = NULL;

	logDebug("number of operator families in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		f[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		f[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "opfnspname")));
		f[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "opfname")));
		f[i].accessmethod = strdup(PQgetvalue(res, i, PQfnumber(res, "amname")));

		/*
		 * These values are not assigned here (see getOpFuncAttributes), but
		 * default values are essential to avoid having trouble in
		 * freeOperatorFamilies.
		 */
		f[i].opandfunc.noperators = 0;
		f[i].opandfunc.operators = NULL;
		f[i].opandfunc.nfunctions = 0;
		f[i].opandfunc.functions = NULL;

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			f[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			f[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (f[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		f[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "opfowner")));
	}

	PQclear(res);

	return f;
}

void
getOpFuncAttributes(PGconn *c, Oid o, PQLOpAndFunc *d)
{
	char		*query;
	PGresult	*res;
	int			i;

	/* Operators */

	query = psprintf("SELECT amopopr::regoperator, amopstrategy, f.oid AS opfoid, n.nspname AS opfnspname, f.opfname FROM pg_amop a LEFT JOIN (pg_opfamily f INNER JOIN pg_namespace n ON (f.opfnamespace = n.oid)) ON (a.amopsortfamily = f.oid) WHERE a.amopfamily = %u", o);

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

	d->noperators = PQntuples(res);
	if (d->noperators > 0)
		d->operators = (PQLOpOperators *) malloc(d->noperators * sizeof(
						   PQLOpOperators));
	else
		d->operators = NULL;

	logDebug("number of operators in operator class \"%u\": %d", o, d->noperators);

	for (i = 0; i < d->noperators; i++)
	{
		d->operators[i].strategy = strtoul(PQgetvalue(res, i, PQfnumber(res,
										   "amopstrategy")),
										   NULL, 10);
		d->operators[i].oprname = strdup(PQgetvalue(res, i, PQfnumber(res, "amopopr")));
		if (PQgetisnull(res, i, PQfnumber(res, "opfname")))
		{
			d->operators[i].sortfamily.oid = 0;
			d->operators[i].sortfamily.schemaname = NULL;
			d->operators[i].sortfamily.objectname = NULL;
		}
		else
		{
			d->operators[i].sortfamily.oid = strtoul(PQgetvalue(res, i, PQfnumber(res,
											 "opfoid")), NULL, 10);
			d->operators[i].sortfamily.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
													"opfnspname")));
			d->operators[i].sortfamily.objectname = strdup(PQgetvalue(res, i, PQfnumber(res,
													"opfname")));
		}

		logDebug("operator: \"%s\".\"%s\" ; strategy %d",
				 d->operators[i].oprname,
				 d->operators[i].strategy);
	}

	PQclear(res);

	/* Functions */

	query = psprintf("SELECT amproc::regprocedure, amprocnum FROM pg_amop WHERE amopfamily = %u", o);

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

	d->nfunctions = PQntuples(res);
	if (d->nfunctions > 0)
		d->functions = (PQLOpFunctions *) malloc(d->nfunctions * sizeof(
						   PQLOpFunctions));
	else
		d->functions = NULL;

	logDebug("number of functions in operator class \"%u\": %d", o, d->nfunctions);

	for (i = 0; i < d->nfunctions; i++)
	{
		d->functions[i].support = strtoul(PQgetvalue(res, i, PQfnumber(res,
										  "amprocnum")),
										  NULL, 10);
		d->functions[i].funcname = strdup(PQgetvalue(res, i, PQfnumber(res, "amproc")));

		logDebug("function: \"%s\" ; support %d",
				 d->functions[i].funcname,
				 d->functions[i].support);
	}

	PQclear(res);
}

int
compareOperators(PQLOperator *a, PQLOperator *b)
{
	int		c;

	c = strcmp(a->obj.schemaname, b->obj.schemaname);

	/* compare operator names iif schema are equal */
	if (c == 0)
	{
		c = strcmp(a->obj.objectname, b->obj.objectname);
		/* compare operands iif objectname are equal */
		if (c == 0)
			c = (strcmp(a->lefttype, b->lefttype) ||
				 strcmp(a->righttype, b->righttype));
	}

	return c;
}

void
freeOperators(PQLOperator *o, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(o[i].obj.schemaname);
			free(o[i].obj.objectname);
			free(o[i].procedure);
			if (o[i].lefttype)
				free(o[i].lefttype);
			if (o[i].righttype)
				free(o[i].righttype);
			if (o[i].commutator)
				free(o[i].commutator);
			if (o[i].negator)
				free(o[i].negator);
			if (o[i].restriction)
				free(o[i].restriction);
			if (o[i].join)
				free(o[i].join);
			if (o[i].comment)
				PQfreemem(o[i].comment);
			free(o[i].owner);
		}

		free(o);
	}
}

void
freeOperatorClasses(PQLOperatorClass *c, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(c[i].obj.schemaname);
			free(c[i].obj.objectname);
			free(c[i].intype);
			free(c[i].accessmethod);
			if (c[i].family.schemaname)
				free(c[i].family.schemaname);
			if (c[i].family.objectname)
				free(c[i].family.objectname);
			if (c[i].storagetype)
				free(c[i].storagetype);

			/* operators */
			for (j = 0; j < c[i].opandfunc.noperators; j++)
			{
				free(c[i].opandfunc.operators[j].oprname);
				if (c[i].opandfunc.operators[j].sortfamily.schemaname)
					free(c[i].opandfunc.operators[j].sortfamily.schemaname);
				if (c[i].opandfunc.operators[j].sortfamily.objectname)
					free(c[i].opandfunc.operators[j].sortfamily.objectname);
			}

			/* functions */
			for (j = 0; j < c[i].opandfunc.nfunctions; j++)
				free(c[i].opandfunc.functions[j].funcname);

			if (c[i].comment)
				PQfreemem(c[i].comment);
			free(c[i].owner);
		}

		free(c);
	}
}

void
freeOperatorFamilies(PQLOperatorFamily *f, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(f[i].obj.schemaname);
			free(f[i].obj.objectname);
			free(f[i].accessmethod);

			/* operators */
			for (j = 0; j < f[i].opandfunc.noperators; j++)
			{
				free(f[i].opandfunc.operators[j].oprname);
				if (f[i].opandfunc.operators[j].sortfamily.schemaname)
					free(f[i].opandfunc.operators[j].sortfamily.schemaname);
				if (f[i].opandfunc.operators[j].sortfamily.objectname)
					free(f[i].opandfunc.operators[j].sortfamily.objectname);
			}

			/* functions */
			for (j = 0; j < f[i].opandfunc.nfunctions; j++)
				free(f[i].opandfunc.functions[j].funcname);

			if (f[i].comment)
				PQfreemem(f[i].comment);
			free(f[i].owner);
		}

		free(f);
	}
}

void
dumpDropOperator(FILE *output, PQLOperator *o)
{
	char	*schema = formatObjectIdentifier(o->obj.schemaname);
	char	*oprname = o->obj.objectname;

	fprintf(output, "\n\n");
	fprintf(output, "DROP OPERATOR %s.%s(%s,%s);",
			schema, oprname,
			(o->lefttype) ? o->lefttype : "NONE",
			(o->righttype) ? o->righttype : "NONE");

	free(schema);
}

void
dumpDropOperatorClass(FILE *output, PQLOperatorClass *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*opcname = formatObjectIdentifier(c->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP OPERATOR CLASS %s.%s USING %s;", schema, opcname,
			c->accessmethod);

	free(schema);
	free(opcname);
}

void
dumpDropOperatorFamily(FILE *output, PQLOperatorFamily *f)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*opfname = formatObjectIdentifier(f->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP OPERATOR FAMILY %s.%s USING %s;", schema, opfname,
			f->accessmethod);

	free(schema);
	free(opfname);
}

void
dumpCreateOperator(FILE *output, PQLOperator *o)
{
	char	*schema = formatObjectIdentifier(o->obj.schemaname);
	char	*oprname = o->obj.objectname;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE OPERATOR %s.%s (", schema, oprname);
	fprintf(output, "\nPROCEDURE = %s", o->procedure);
	if (o->lefttype)
		fprintf(output, ",\nLEFTARG = %s", (o->lefttype) ? o->lefttype : "NONE");
	if (o->righttype)
		fprintf(output, ",\nRIGHTARG = %s", (o->righttype) ? o->righttype : "NONE");
	if (o->commutator)
		fprintf(output, ",\nCOMMUTATOR = %s", o->commutator);
	if (o->negator)
		fprintf(output, ",\nNEGATOR = %s", o->negator);
	if (o->restriction)
		fprintf(output, ",\nRESTRICT = %s", o->restriction);
	if (o->join)
		fprintf(output, ",\nJOIN = %s", o->join);
	if (o->canhash)
		fprintf(output, ",\nHASHES");
	if (o->canmerge)
		fprintf(output, ",\nMERGES");
	fprintf(output, ");");

	/* comment */
	if (options.comment)
	{
		if (o->comment != NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON OPERATOR %s.%s(%s,%s) IS %s;",
					schema, oprname,
					(o->lefttype) ? o->lefttype : "NONE",
					(o->righttype) ? o->righttype : "NONE",
					o->comment);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(o->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER OPERATOR %s.%s(%s,%s) OWNER TO %s;",
				schema, oprname,
				(o->lefttype) ? o->lefttype : "NONE",
				(o->righttype) ? o->righttype : "NONE",
				owner);

		free(owner);
	}

	free(schema);
}

void
dumpCreateOperatorClass(FILE *output, PQLOperatorClass *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*opcname = formatObjectIdentifier(c->obj.objectname);
	char	*fschema = NULL;
	char	*opfname = NULL;
	bool	comma = false;
	int		i;

	/* family could be null */
	if (c->family.schemaname != NULL)
		fschema = formatObjectIdentifier(c->family.schemaname);
	if (c->family.objectname != NULL)
		opfname = formatObjectIdentifier(c->family.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE OPERATOR CLASS %s.%s", schema, opcname);
	if (c->defaultopclass)
		fprintf(output, " DEFAULT");
	fprintf(output, " FOR TYPE %s USING %s", c->intype, c->accessmethod);
	if (fschema != NULL && opfname != NULL)
		fprintf(output, " FAMILY %s.%s", fschema, opfname);
	fprintf(output, " AS");

	if (c->storagetype)
	{
		fprintf(output, " STORAGE %s", c->storagetype);
		comma = true;
	}

	/* operators */
	for (i = 0; i < c->opandfunc.noperators; i++)
	{
		char	*tmps = NULL;
		char	*tmpo = NULL;

		if (comma)
			fprintf(output, ",\n");
		else
			comma = true;
		fprintf(output, " OPERATOR %d %s", c->opandfunc.operators[i].strategy,
				c->opandfunc.operators[i].oprname);
		if (c->opandfunc.operators[i].sortfamily.objectname)
		{
			tmps = formatObjectIdentifier(c->opandfunc.operators[i].sortfamily.schemaname);
			tmpo = formatObjectIdentifier(c->opandfunc.operators[i].sortfamily.objectname);
			fprintf(output, " FOR ORDER BY %s.%s", tmps, tmpo);
		}

		if (tmps)
			free(tmps);
		if (tmpo)
			free(tmpo);
	}

	/* functions */
	for (i = 0; i < c->opandfunc.nfunctions; i++)
	{
		if (comma)
			fprintf(output, ",\n");
		else
			comma = true;
		fprintf(output, " FUNCTION %d %s", c->opandfunc.functions[i].support,
				c->opandfunc.functions[i].funcname);
	}
	fprintf(output, ";");

	/* comment */
	if (options.comment)
	{
		if (c->comment != NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON OPERATOR CLASS %s.%s USING %s IS %s;",
					schema, opcname,
					c->accessmethod,
					c->comment);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(c->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER OPERATOR CLASS %s.%s USING %s OWNER TO %s;",
				schema, opcname,
				c->accessmethod,
				owner);

		free(owner);
	}

	free(schema);
	free(opcname);
	if (fschema)
		free(fschema);
	if (opfname)
		free(opfname);
}

void
dumpCreateOperatorFamily(FILE *output, PQLOperatorFamily *f)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*opfname = formatObjectIdentifier(f->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE OPERATOR FAMILY %s.%s USING %s;", schema, opfname,
			f->accessmethod);

	/* comment */
	if (options.comment)
	{
		if (f->comment != NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON OPERATOR FAMILY %s.%s USING %s IS %s;",
					schema, opfname,
					f->accessmethod,
					f->comment);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(f->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER OPERATOR FAMILY %s.%s USING %s OWNER TO %s;",
				schema, opfname,
				f->accessmethod,
				owner);

		free(owner);
	}

	free(schema);
	free(opfname);
}

void
dumpAlterOperator(FILE *output, PQLOperator *a, PQLOperator *b)
{
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*oprname2 = b->obj.objectname;
	char	*r = NULL;
	char	*j = NULL;

	/* restriction/join selectivity estimator */
	if ((a->restriction == NULL && b->restriction != NULL) ||
			(a->restriction != NULL && b->restriction != NULL &&
			 strcmp(a->restriction, b->restriction) != 0))
		r = strdup(b->restriction);
	else if (a->restriction != NULL && b->restriction == NULL)
		r = strdup("NONE");

	if ((a->join == NULL && b->join != NULL) ||
			(a->join != NULL && b->join != NULL &&
			 strcmp(a->join, b->join) != 0))
		j = strdup(b->join);
	else if (a->join != NULL && b->join == NULL)
		j = strdup("NONE");

	if (r != NULL || j != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER OPERATOR %s.%s(%s,%s) SET (",
				schema2,
				oprname2,
				(b->lefttype) ? b->lefttype : "NONE",
				(b->righttype) ? b->righttype : "NONE");

		if (r != NULL)
			fprintf(output, "RESTRICT = %s", r);
		if (r != NULL && j != NULL)
			fprintf(output, ", ");
		if (j != NULL)
			fprintf(output, "JOIN = %s", j);
		fprintf(output, ");");
	}

	if (r)
		free(r);
	if (j)
		free(j);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT OPERATOR %s.%s(%s,%s) IS '%s';",
					schema2,
					oprname2,
					(b->lefttype) ? b->lefttype : "NONE",
					(b->righttype) ? b->righttype : "NONE",
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT OPERATOR %s.%s(%s,%s) IS NULL;",
					schema2,
					oprname2,
					(b->lefttype) ? b->lefttype : "NONE",
					(b->righttype) ? b->righttype : "NONE");
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER OPERATOR %s.%s(%s,%s) OWNER TO %s;",
					schema2,
					oprname2,
					(b->lefttype) ? b->lefttype : "NONE",
					(b->righttype) ? b->righttype : "NONE",
					owner);

			free(owner);
		}
	}

	free(schema2);
}

static void
dumpAddOperatorOpFamily(FILE *output, PQLOperatorFamily *f, int i)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*opfname = formatObjectIdentifier(f->obj.objectname);
	char	*tmps = NULL;
	char	*tmpo = NULL;

	fprintf(output, "\n\n");
	fprintf(output, "ALTER OPERATOR FAMILY %s.%s USING %s ADD OPERATOR %d %s",
			schema, opfname, f->accessmethod,
			f->opandfunc.operators[i].strategy,
			f->opandfunc.operators[i].oprname);
	if (f->opandfunc.operators[i].sortfamily.objectname)
	{
		tmps = formatObjectIdentifier(f->opandfunc.operators[i].sortfamily.schemaname);
		tmpo = formatObjectIdentifier(f->opandfunc.operators[i].sortfamily.objectname);
		fprintf(output, " FOR ORDER BY %s.%s", tmps, tmpo);
	}
	fprintf(output, ";");

	if (tmps)
		free(tmps);
	if (tmpo)
		free(tmpo);

	free(schema);
	free(opfname);
}

static void
dumpRemoveOperatorOpFamily(FILE *output, PQLOperatorFamily *f, int i)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*opfname = formatObjectIdentifier(f->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER OPERATOR FAMILY %s.%s USING %s DROP OPERATOR %d;",
			schema, opfname, f->accessmethod,
			f->opandfunc.operators[i].strategy);

	free(schema);
	free(opfname);
}

static void
dumpAddFunctionOpFamily(FILE *output, PQLOperatorFamily *f, int i)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*opfname = formatObjectIdentifier(f->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER OPERATOR FAMILY %s.%s USING %s ADD FUNCTION %d %s;",
			schema, opfname, f->accessmethod,
			f->opandfunc.functions[i].support,
			f->opandfunc.functions[i].funcname);

	free(schema);
	free(opfname);
}

static void
dumpRemoveFunctionOpFamily(FILE *output, PQLOperatorFamily *f, int i)
{
	char	*schema = formatObjectIdentifier(f->obj.schemaname);
	char	*opfname = formatObjectIdentifier(f->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER OPERATOR FAMILY %s.%s USING %s DROP FUNCTION %d;",
			schema, opfname, f->accessmethod,
			f->opandfunc.functions[i].support);

	free(schema);
	free(opfname);
}

void
dumpAlterOperatorClass(FILE *output, PQLOperatorClass *a, PQLOperatorClass *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*opcname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*opcname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT OPERATOR CLASS %s.%s USING %s IS '%s';",
					schema2,
					opcname2,
					b->accessmethod,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT OPERATOR CLASS %s.%s USING %s IS NULL;",
					schema2,
					opcname2,
					b->accessmethod);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER OPERATOR CLASS %s.%s USING %s OWNER TO %s;",
					schema2,
					opcname2,
					b->accessmethod,
					owner);

			free(owner);
		}
	}

	free(schema1);
	free(opcname1);
	free(schema2);
	free(opcname2);
}

void
dumpAlterOperatorFamily(FILE *output, PQLOperatorFamily *a,
						PQLOperatorFamily *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*opfname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*opfname2 = formatObjectIdentifier(b->obj.objectname);

	int		i, j;

	/* the operators are sorted by strategy */
	i = j = 0;
	while (i < a->opandfunc.noperators || j < b->opandfunc.noperators)
	{
		/*
		 * End of operators from operator class a. Additional operators from
		 * operator class b will be added.
		 */
		if (i == a->opandfunc.noperators)
		{
			logDebug("operator class \"%s\".\"%s\" operator \"%s\" added",
					 b->obj.schemaname, b->obj.objectname, b->opandfunc.operators[j].oprname);
			dumpAddOperatorOpFamily(output, b, j);
			j++;
		}
		/*
		 * End of operators from operator class b. Additional operators from
		 * operator class a will be removed.
		 */
		else if (j == b->opandfunc.noperators)
		{
			logDebug("operator class \"%s\".\"%s\" operator \"%s\" removed",
					 a->obj.schemaname, a->obj.objectname, a->opandfunc.operators[i].oprname);
			dumpRemoveOperatorOpFamily(output, a, i);
			i++;
		}
		else if (a->opandfunc.operators[i].strategy ==
				 b->opandfunc.operators[j].strategy)
		{
			/* do nothing */
			i++;
			j++;
		}
		else if (a->opandfunc.operators[i].strategy <
				 b->opandfunc.operators[j].strategy)
		{
			logDebug("operator class \"%s\".\"%s\" operator \"%s\" removed",
					 a->obj.schemaname, a->obj.objectname, a->opandfunc.operators[i].oprname);
			dumpRemoveOperatorOpFamily(output, a, i);
			i++;
		}
		else if (a->opandfunc.operators[i].strategy >
				 b->opandfunc.operators[j].strategy)
		{
			logDebug("operator class \"%s\".\"%s\" operator \"%s\" added",
					 b->obj.schemaname, b->obj.objectname, b->opandfunc.operators[j].oprname);
			dumpAddOperatorOpFamily(output, b, j);
			j++;
		}
	}

	/* the functions are sorted by support */
	i = j = 0;
	while (i < a->opandfunc.nfunctions || j < b->opandfunc.nfunctions)
	{
		/*
		 * End of functions from operator class a. Additional functions from
		 * operator class b will be added.
		 */
		if (i == a->opandfunc.nfunctions)
		{
			logDebug("operator class \"%s\".\"%s\" function \"%s\" added",
					 b->obj.schemaname, b->obj.objectname, b->opandfunc.functions[j].funcname);
			dumpAddFunctionOpFamily(output, b, j);
			j++;
		}
		/*
		 * End of functions from operator class b. Additional functions from
		 * operator class a will be removed.
		 */
		else if (j == b->opandfunc.nfunctions)
		{
			logDebug("operator class \"%s\".\"%s\" function \"%s\" removed",
					 a->obj.schemaname, a->obj.objectname, a->opandfunc.functions[i].funcname);
			dumpRemoveFunctionOpFamily(output, a, i);
			i++;
		}
		else if (a->opandfunc.functions[i].support == b->opandfunc.functions[j].support)
		{
			/* do nothing */
			i++;
			j++;
		}
		else if (a->opandfunc.functions[i].support < b->opandfunc.functions[j].support)
		{
			logDebug("operator class \"%s\".\"%s\" function \"%s\" removed",
					 a->obj.schemaname, a->obj.objectname, a->opandfunc.functions[i].funcname);
			dumpRemoveFunctionOpFamily(output, a, i);
			i++;
		}
		else if (a->opandfunc.functions[i].support > b->opandfunc.functions[j].support)
		{
			logDebug("operator class \"%s\".\"%s\" function \"%s\" added",
					 b->obj.schemaname, b->obj.objectname, b->opandfunc.functions[j].funcname);
			dumpAddFunctionOpFamily(output, b, j);
			j++;
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
			fprintf(output, "COMMENT OPERATOR FAMILY %s.%s USING %s IS '%s';",
					schema2,
					opfname2,
					b->accessmethod,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT OPERATOR FAMILY %s.%s USING %s IS NULL;",
					schema2,
					opfname2,
					b->accessmethod);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER OPERATOR CLASS %s.%s USING %s OWNER TO %s;",
					schema2,
					opfname2,
					b->accessmethod,
					owner);

			free(owner);
		}
	}

	free(schema1);
	free(opfname1);
	free(schema2);
	free(opfname2);
}
