/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * aggregate.c
 *     Generate AGGREGATE commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE AGGREGATE
 * DROP AGGREGATE
 * ALTER AGGREGATE
 * COMMENT ON AGGREGATE
 *
 * TODO
 *
 * ALTER AGGREGATE ... RENAME TO
 * ALTER AGGREGATE ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "aggregate.h"


PQLAggregate *
getAggregates(PGconn *c, int *n)
{
	PQLAggregate	*a;
	PGresult		*res;
	int				i;

	logNoise("aggregate: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 110000)
		res = PQexec(c,
					 "SELECT p.oid, n.nspname, p.proname, pg_get_function_arguments(p.oid) AS aggargs, aggtransfn, aggtranstype::regtype, aggtransspace, aggfinalfn, aggfinalextra, aggfinalmodify, agginitval, aggmtransfn, aggminvtransfn, aggmtranstype::regtype, aggmtransspace, aggmfinalfn, aggmfinalextra, aggmfinalmodify, aggminitval, aggsortop::regoperator, proparallel, (aggkind = 'h') AS hypothetical, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(p.proowner) AS aggowner FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) INNER JOIN pg_aggregate a ON (aggfnoid = p.oid) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, p.proname, pg_get_function_arguments(p.oid)");
	else if (PQserverVersion(c) >= 90600)	/* parallel is new in 9.6 */
		res = PQexec(c,
					 "SELECT p.oid, n.nspname, p.proname, pg_get_function_arguments(p.oid) AS aggargs, aggtransfn, aggtranstype::regtype, aggtransspace, aggfinalfn, aggfinalextra, 'n' AS aggfinalmodify, agginitval, aggmtransfn, aggminvtransfn, aggmtranstype::regtype, aggmtransspace, aggmfinalfn, aggmfinalextra, 'n' AS aggmfinalmodify, aggminitval, aggsortop::regoperator, proparallel, (aggkind = 'h') AS hypothetical, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(p.proowner) AS aggowner FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) INNER JOIN pg_aggregate a ON (aggfnoid = p.oid) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, p.proname, pg_get_function_arguments(p.oid)");
	else if (PQserverVersion(c) >= 90400)
		res = PQexec(c,
					 "SELECT p.oid, n.nspname, p.proname, pg_get_function_arguments(p.oid) AS aggargs, aggtransfn, aggtranstype::regtype, aggtransspace, aggfinalfn, aggfinalextra, 'n' AS aggfinalmodify, agginitval, aggmtransfn, aggminvtransfn, aggmtranstype::regtype, aggmtransspace, aggmfinalfn, aggmfinalextra, 'n' AS aggmfinalmodify, aggminitval, aggsortop::regoperator, 'n' AS proparallel, (aggkind = 'h') AS hypothetical, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(p.proowner) AS aggowner FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) INNER JOIN pg_aggregate a ON (aggfnoid = p.oid) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, p.proname, pg_get_function_arguments(p.oid)");
	else if (PQserverVersion(c) >= 90100)	/* extension support */
		res = PQexec(c,
					 "SELECT p.oid, n.nspname, p.proname, pg_get_function_arguments(p.oid) AS aggargs, aggtransfn, aggtranstype::regtype, NULL AS aggtransspace, aggfinalfn, false AS aggfinalextra, 'n' AS aggfinalmodify, agginitval, NULL AS aggmtransfn, NULL AS aggminvtransfn, NULL AS aggmtranstype, NULL AS aggmtransspace, NULL AS aggmfinalfn, false AS aggmfinalextra, 'n' AS aggmfinalmodify, NULL AS aggminitval, aggsortop::regoperator, 'n' AS proparallel, false AS hypothetical, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(p.proowner) AS aggowner FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) INNER JOIN pg_aggregate a ON (aggfnoid = p.oid) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE p.oid = d.objid AND d.deptype = 'e') ORDER BY n.nspname, p.proname, pg_get_function_arguments(p.oid)");
	else
		res = PQexec(c,
					 "SELECT p.oid, n.nspname, p.proname, pg_get_function_arguments(p.oid) AS aggargs, aggtransfn, aggtranstype::regtype, NULL AS aggtransspace, aggfinalfn, false AS aggfinalextra, 'n' AS aggfinalmodify, agginitval, NULL AS aggmtransfn, NULL AS aggminvtransfn, NULL AS aggmtranstype, NULL AS aggmtransspace, NULL AS aggmfinalfn, false AS aggmfinalextra, 'n' AS aggmfinalmodify, NULL AS aggminitval, aggsortop::regoperator, 'n' AS proparallel, false AS hypothetical, obj_description(p.oid, 'pg_proc') AS description, pg_get_userbyid(p.proowner) AS aggowner FROM pg_proc p INNER JOIN pg_namespace n ON (n.oid = p.pronamespace) INNER JOIN pg_aggregate a ON (aggfnoid = p.oid) WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY n.nspname, p.proname, pg_get_function_arguments(p.oid)");

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
		a = (PQLAggregate *) malloc(*n * sizeof(PQLAggregate));
	else
		a = NULL;

	logDebug("number of aggregates in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		a[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		a[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		a[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "proname")));
		a[i].arguments = strdup(PQgetvalue(res, i, PQfnumber(res, "aggargs")));
		a[i].sfunc  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggtransfn")));
		a[i].stype  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggtranstype")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggtransspace")))
			a[i].sspace = NULL;
		else
			a[i].sspace  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggtransspace")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggfinalfn")))
			a[i].finalfunc = NULL;
		else
			a[i].finalfunc  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggfinalfn")));

		a[i].finalfuncextra = (PQgetvalue(res, i, PQfnumber(res,
										  "aggfinalextra"))[0] == 't');

		a[i].finalfuncmodify = PQgetvalue(res, i, PQfnumber(res, "aggfinalmodify"))[0];

		if (PQgetisnull(res, i, PQfnumber(res, "agginitval")))
			a[i].initcond = NULL;
		else
			a[i].initcond  = strdup(PQgetvalue(res, i, PQfnumber(res, "agginitval")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggmtransfn")))
			a[i].msfunc = NULL;
		else
			a[i].msfunc  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggmtransfn")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggminvtransfn")))
			a[i].minvfunc = NULL;
		else
			a[i].minvfunc  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggminvtransfn")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggmtranstype")))
			a[i].mstype = NULL;
		else
			a[i].mstype  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggmtranstype")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggmtransspace")))
			a[i].msspace = NULL;
		else
			a[i].msspace  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggmtransspace")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggmfinalfn")))
			a[i].mfinalfunc = NULL;
		else
			a[i].mfinalfunc  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggmfinalfn")));

		a[i].mfinalfuncextra = (PQgetvalue(res, i, PQfnumber(res,
										   "aggmfinalextra"))[0] == 't');

		a[i].mfinalfuncmodify = PQgetvalue(res, i, PQfnumber(res, "aggmfinalmodify"))[0];

		if (PQgetisnull(res, i, PQfnumber(res, "aggminitval")))
			a[i].minitcond = NULL;
		else
			a[i].minitcond  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggminitval")));

		if (PQgetisnull(res, i, PQfnumber(res, "aggsortop")))
			a[i].sortop = NULL;
		else
			a[i].sortop  = strdup(PQgetvalue(res, i, PQfnumber(res, "aggsortop")));

		a[i].parallel = PQgetvalue(res, i, PQfnumber(res, "proparallel"))[0];
		a[i].hypothetical = (PQgetvalue(res, i, PQfnumber(res,
										"hypothetical"))[0] == 't');

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			a[i].comment = NULL;
		else
			a[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		a[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "aggowner")));

		/*
		 * Security labels are not assigned here (see getAggregateSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeAggregates.
		 */
		a[i].nseclabels = 0;
		a[i].seclabels = NULL;

		logDebug("aggregate \"%s\".\"%s\"(%s)", a[i].obj.schemaname,
				 a[i].obj.objectname, a[i].arguments);
	}

	PQclear(res);

	return a;
}

/*
 * TODO this function is similar to compareFunctions(). Combine them.
 */
int
compareAggregates(PQLAggregate *a, PQLAggregate *b)
{
	int		c;

	c = strcmp(a->obj.schemaname, b->obj.schemaname);

	/* compare relation names iif schema names are equal */
	if (c == 0)
	{
		c = strcmp(a->obj.objectname, b->obj.objectname);

		/* compare arguments iif schema-qualified names are equal */
		if (c == 0)
			c = strcmp(a->arguments, b->arguments);
	}

	return c;
}

void
getAggregateSecurityLabels(PGconn *c, PQLAggregate *a)
{
	char		query[200];
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	snprintf(query, 200,
			 "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_aggregate' AND s.objoid = %u ORDER BY provider",
			 a->obj.oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	a->nseclabels = PQntuples(res);
	if (a->nseclabels > 0)
		a->seclabels = (PQLSecLabel *) malloc(a->nseclabels * sizeof(PQLSecLabel));
	else
		a->seclabels = NULL;

	logDebug("number of security labels in aggregate \"%s\".\"%s\"(%s): %d",
			 a->obj.schemaname, a->obj.objectname, a->arguments, a->nseclabels);

	for (i = 0; i < a->nseclabels; i++)
	{
		a->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		a->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
	}

	PQclear(res);
}

void
freeAggregates(PQLAggregate *a, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(a[i].obj.schemaname);
			free(a[i].obj.objectname);
			free(a[i].arguments);
			free(a[i].sfunc);
			free(a[i].stype);
			if (a[i].sspace)
				free(a[i].sspace);
			if (a[i].finalfunc)
				free(a[i].finalfunc);
			if (a[i].initcond)
				free(a[i].initcond);
			if (a[i].msfunc)
				free(a[i].msfunc);
			if (a[i].minvfunc)
				free(a[i].minvfunc);
			if (a[i].mstype)
				free(a[i].mstype);
			if (a[i].msspace)
				free(a[i].msspace);
			if (a[i].mfinalfunc)
				free(a[i].mfinalfunc);
			if (a[i].minitcond)
				free(a[i].minitcond);
			if (a[i].sortop)
				free(a[i].sortop);
			if (a[i].comment)
				free(a[i].comment);
			free(a[i].owner);

			/* security labels */
			for (j = 0; j < a[i].nseclabels; j++)
			{
				free(a[i].seclabels[j].provider);
				free(a[i].seclabels[j].label);
			}

			if (a[i].seclabels)
				free(a[i].seclabels);
		}

		free(a);
	}
}

void
dumpDropAggregate(FILE *output, PQLAggregate *a)
{
	char	*schema = formatObjectIdentifier(a->obj.schemaname);
	char	*aggname = formatObjectIdentifier(a->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP AGGREGATE %s.%s(%s);",
			schema, aggname, a->arguments);

	free(schema);
	free(aggname);
}

void
dumpCreateAggregate(FILE *output, PQLAggregate *a)
{
	char	*schema = formatObjectIdentifier(a->obj.schemaname);
	char	*aggname = formatObjectIdentifier(a->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE AGGREGATE %s.%s(%s) (",
			schema, aggname, a->arguments);
	fprintf(output, "\nSFUNC = %s", a->sfunc);
	fprintf(output, ",\nSTYPE = %s,", a->stype);
	if (a->sspace)
		fprintf(output, ",\nSSPACE = %s", a->sspace);
	if (a->finalfunc)
	{
		fprintf(output, ",\nFINALFUNC = %s", a->finalfunc);
		if (a->finalfuncextra)
			fprintf(output, ",\nFINALFUNC_EXTRA");
		/*
		 * 'n' means unsupported feature (<= 10)
		 */
		if (a->finalfuncmodify == 's')
			fprintf(output, ",\nFINALFUNC_MODIFY = SHAREABLE");
		else if (a->finalfuncmodify == 'w')
			fprintf(output, ",\nFINALFUNC_MODIFY = READ_WRITE");
		else if (a->finalfuncmodify == 'r')	/* FINALFUNC_MODIFY = READ_ONLY is the default */
			;
	}
	if (a->initcond)
		fprintf(output, ",\nINITCOND = %s", a->initcond);
	if (a->msfunc)
		fprintf(output, ",\nMSFUNC = %s", a->msfunc);
	if (a->minvfunc)
		fprintf(output, ",\nMINVFUNC = %s", a->minvfunc);
	if (a->mstype)
		fprintf(output, ",\nMSTYPE = %s,", a->mstype);
	if (a->msspace)
		fprintf(output, ",\nMSSPACE = %s", a->msspace);
	if (a->mfinalfunc)
	{
		fprintf(output, ",\nMFINALFUNC = %s", a->mfinalfunc);
		if (a->mfinalfuncextra)
			fprintf(output, ",\nMFINALFUNC_EXTRA");
		/*
		 * 'n' means unsupported feature (<= 10)
		 */
		if (a->mfinalfuncmodify == 's')
			fprintf(output, ",\nMFINALFUNC_MODIFY = SHAREABLE");
		else if (a->mfinalfuncmodify == 'w')
			fprintf(output, ",\nMFINALFUNC_MODIFY = READ_WRITE");
		else if (a->mfinalfuncmodify == 'r')	/* MFINALFUNC_MODIFY = READ_ONLY is the default */
			;
	}
	if (a->minitcond)
		fprintf(output, ",\nMINITCOND = %s", a->minitcond);
	if (a->sortop)
		fprintf(output, ",\nSORTOP = %s", a->sortop);
	if (a->hypothetical)
		fprintf(output, ",\nHYPOTHETICAL");
	/*
	 * 'n' means unsupported feature (<= 9.5)
	 */
	if (a->parallel == 's')
		fprintf(output, ",\nPARALLEL = SAFE");
	else if (a->parallel == 'r')
		fprintf(output, ",\nPARALLEL = RESTRICTED");
	else if (a->parallel == 'u')	/* PARALLEL = UNSAFE is the default */
		;

	fprintf(output, ");");

	free(schema);
	free(aggname);
}

void
dumpAlterAggregate(FILE *output, PQLAggregate *a, PQLAggregate *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*aggname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*aggname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON AGGREGATE %s.%s(%s) IS '%s';",
					schema2, aggname2, b->arguments, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON AGGREGATE %s.%s(%s) IS NULL;",
					schema2, aggname2, b->arguments);
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
				fprintf(output, "SECURITY LABEL FOR %s ON AGGREGATE %s.%s(%s) IS '%s';",
						b->seclabels[i].provider,
						schema2,
						aggname2,
						b->arguments,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON AGGREGATE %s.%s(%s) IS NULL;",
						a->seclabels[i].provider,
						schema1,
						aggname1,
						a->arguments);
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
					fprintf(output, "SECURITY LABEL FOR %s ON AGGREGATE %s.%s(%s) IS '%s';",
							b->seclabels[j].provider,
							schema2,
							aggname2,
							b->arguments,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON AGGREGATE %s.%s(%s) IS NULL;",
							a->seclabels[i].provider,
							schema1,
							aggname1,
							a->arguments);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON AGGREGATE %s.%s(%s) IS '%s';",
								b->seclabels[j].provider,
								schema2,
								aggname2,
								b->arguments,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON AGGREGATE %s.%s(%s) IS NULL;",
							a->seclabels[i].provider,
							schema1,
							aggname1,
							a->arguments);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON AGGREGATE %s.%s(%s) IS '%s';",
							b->seclabels[j].provider,
							schema2,
							aggname2,
							b->arguments,
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
			fprintf(output, "ALTER AGGREGATE %s.%s(%s) OWNER TO %s;",
					schema2,
					aggname2,
					b->arguments,
					b->owner);
		}
	}

	free(schema1);
	free(aggname1);
	free(schema2);
	free(aggname2);
}
