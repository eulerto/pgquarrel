/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * language.c
 *     Generate LANGUAGE commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE LANGUAGE
 * DROP LANGUAGE
 * ALTER LANGUAGE
 * COMMENT ON LANGUAGE
 *
 * TODO
 *
 * ALTER LANGUAGE ... RENAME TO
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "language.h"


PQLLanguage *
getLanguages(PGconn *c, int *n)
{
	PQLLanguage	*l;
	PGresult	*res;
	int			i;

	logNoise("language: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		res = PQexec(c,
					 "SELECT l.oid, lanname AS languagename, lanpltrusted AS trusted, p1.oid AS calloid, p1.pronamespace::regnamespace AS callnsp, p1.proname AS callname, p2.oid AS inlineoid, p2.pronamespace::regnamespace AS inlinensp, p2.proname AS inlinename, p3.oid AS validatoroid, p3.pronamespace::regnamespace AS validatornsp, p3.proname AS validatorname, obj_description(l.oid, 'pg_language') AS description, pg_get_userbyid(lanowner) AS lanowner, lanacl FROM pg_language l LEFT JOIN pg_proc p1 ON (p1.oid = lanplcallfoid) LEFT JOIN pg_proc p2 ON (p2.oid = laninline) LEFT JOIN pg_proc p3 ON (p3.oid = lanvalidator) WHERE lanispl AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE l.oid = d.objid AND d.deptype = 'e') ORDER BY lanname");
	}
	else
	{
		res = PQexec(c,
					 "SELECT l.oid, lanname AS languagename, lanpltrusted AS trusted, p1.oid AS calloid, p1.pronamespace::regnamespace AS callnsp, p1.proname AS callfunc, p2.oid AS inlineoid, p2.pronamespace::regnamespace AS inlinensp, p2.proname AS inlinefunc, p3.oid AS validatoroid, p3.pronamespace::regnamespace AS validatornsp, p3.proname AS validatorfunc, obj_description(l.oid, 'pg_language') AS description, pg_get_userbyid(lanowner) AS lanowner, lanacl FROM pg_language l LEFT JOIN pg_proc p1 ON (p1.oid = lanplcallfoid) LEFT JOIN pg_proc p2 ON (p2.oid = laninline) LEFT JOIN pg_proc p3 ON (p3.oid = lanvalidator) WHERE lanispl ORDER BY lanname");
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
		l = (PQLLanguage *) malloc(*n * sizeof(PQLLanguage));
	else
		l = NULL;

	logDebug("number of languages in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		l[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		l[i].languagename = strdup(PQgetvalue(res, i, PQfnumber(res, "languagename")));
		l[i].trusted = (PQgetvalue(res, i, PQfnumber(res, "trusted"))[0] == 't');

		if (PQgetisnull(res, i, PQfnumber(res, "callname")))
		{
			l[i].callfunc.oid = InvalidOid;
			l[i].callfunc.schemaname = NULL;
			l[i].callfunc.objectname = NULL;
		}
		else
		{
			l[i].callfunc.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "calloid")), NULL, 10);
			l[i].callfunc.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "callnsp")));
			l[i].callfunc.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "callname")));
		}
		if (PQgetisnull(res, i, PQfnumber(res, "inlinename")))
		{
			l[i].inlinefunc.oid = InvalidOid;
			l[i].inlinefunc.schemaname = NULL;
			l[i].inlinefunc.objectname = NULL;
		}
		else
		{
			l[i].inlinefunc.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "inlineoid")), NULL, 10);
			l[i].inlinefunc.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "inlinensp")));
			l[i].inlinefunc.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "inlinename")));
		}
		if (PQgetisnull(res, i, PQfnumber(res, "validatorname")))
		{
			l[i].validatorfunc.oid = InvalidOid;
			l[i].validatorfunc.schemaname = NULL;
			l[i].validatorfunc.objectname = NULL;
		}
		else
		{
			l[i].validatorfunc.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "validatoroid")), NULL, 10);
			l[i].validatorfunc.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "validatornsp")));
			l[i].validatorfunc.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "validatorname")));
		}

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			l[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			l[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (l[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		l[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "lanowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "lanacl")))
			l[i].acl = NULL;
		else
			l[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "lanacl")));

		/*
		 * Security labels are not assigned here (see
		 * getLanguageSecurityLabels), but default values are essential to
		 * avoid having trouble in freeLanguages.
		 */
		l[i].nseclabels = 0;
		l[i].seclabels = NULL;

		logDebug("language \"%s\"", l[i].languagename);
	}

	PQclear(res);

	return l;
}

void
getLanguageSecurityLabels(PGconn *c, PQLLanguage *l)
{
	char		*query;
	PGresult	*res;
	int			i;

	if (PQserverVersion(c) < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	query = psprintf("SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_language' AND s.objoid = %u ORDER BY provider", l->oid);

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

	l->nseclabels = PQntuples(res);
	if (l->nseclabels > 0)
		l->seclabels = (PQLSecLabel *) malloc(l->nseclabels * sizeof(PQLSecLabel));
	else
		l->seclabels = NULL;

	logDebug("number of security labels in language \"%s\": %d", l->languagename,
			 l->nseclabels);

	for (i = 0; i < l->nseclabels; i++)
	{
		char	*withoutescape;

		l->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		withoutescape = PQgetvalue(res, i, PQfnumber(res, "label"));
		l->seclabels[i].label = PQescapeLiteral(c, withoutescape,
												strlen(withoutescape));
		if (l->seclabels[i].label == NULL)
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

void
freeLanguages(PQLLanguage *l, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(l[i].languagename);
			if (l[i].callfunc.schemaname)
				free(l[i].callfunc.schemaname);
			if (l[i].callfunc.objectname)
				free(l[i].callfunc.objectname);
			if (l[i].inlinefunc.schemaname)
				free(l[i].inlinefunc.schemaname);
			if (l[i].inlinefunc.objectname)
				free(l[i].inlinefunc.objectname);
			if (l[i].validatorfunc.schemaname)
				free(l[i].validatorfunc.schemaname);
			if (l[i].validatorfunc.objectname)
				free(l[i].validatorfunc.objectname);
			if (l[i].comment)
				PQfreemem(l[i].comment);
			free(l[i].owner);
			if (l[i].acl)
				free(l[i].acl);

			/* security labels */
			for (j = 0; j < l[i].nseclabels; j++)
			{
				free(l[i].seclabels[j].provider);
				PQfreemem(l[i].seclabels[j].label);
			}

			if (l[i].seclabels)
				free(l[i].seclabels);
		}

		free(l);
	}
}

void
dumpDropLanguage(FILE *output, PQLLanguage *l)
{
	char	*langname = formatObjectIdentifier(l->languagename);

	fprintf(output, "\n\n");
	fprintf(output, "DROP LANGUAGE %s;", langname);

	free(langname);
}

void
dumpCreateLanguage(FILE *output, PQLLanguage *l)
{
	char	*langname = formatObjectIdentifier(l->languagename);
	bool	hasparams;
	char	*n1, *n2, *n3;
	char	*o1, *o2, *o3;

	hasparams = (l->callfunc.oid != InvalidOid && l->inlinefunc.oid != InvalidOid && l->validatorfunc.oid != InvalidOid);

	fprintf(output, "\n\n");

	if (hasparams)
	{
		n1 = formatObjectIdentifier(l->callfunc.schemaname);
		o1 = formatObjectIdentifier(l->callfunc.objectname);
		n2 = formatObjectIdentifier(l->inlinefunc.schemaname);
		o2 = formatObjectIdentifier(l->inlinefunc.objectname);
		n3 = formatObjectIdentifier(l->validatorfunc.schemaname);
		o3 = formatObjectIdentifier(l->validatorfunc.objectname);

		fprintf(output, "CREATE %sPROCEDURAL LANGUAGE %s", l->trusted ? "TRUSTED" : "", langname);
		fprintf(output, " HANDLER %s.%s", l->callfunc.schemaname, l->callfunc.objectname);
		fprintf(output, " INLINE %s.%s", l->inlinefunc.schemaname, l->inlinefunc.objectname);
		fprintf(output, " VALIDATOR %s.%s", l->validatorfunc.schemaname, l->validatorfunc.objectname);

		free(n1);
		free(o1);
		free(n2);
		free(o2);
		free(n3);
		free(o3);
	}
	else
	{
		fprintf(output, "CREATE OR REPLACE PROCEDURAL LANGUAGE %s", langname);
	}

	fprintf(output, ";");

	/* comment */
	if (options.comment && l->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON LANGUAGE %s IS %s;", langname, l->comment);
	}

	/* security labels */
	if (options.securitylabels && l->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < l->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS %s;",
					l->seclabels[i].provider,
					langname,
					l->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(l->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER LANGUAGE %s OWNER TO %s;",
				langname,
				owner);

		free(owner);
	}

	/* privileges */
	/* XXX second s.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
	{
		PQLObject tmp;

		tmp.schemaname = NULL;
		tmp.objectname = l->languagename;

		dumpGrantAndRevoke(output, PGQ_LANGUAGE, &tmp, &tmp, NULL, l->acl, NULL, NULL);
	}

	free(langname);
}

void
dumpAlterLanguage(FILE *output, PQLLanguage *a, PQLLanguage *b)
{
	char	*langname1 = formatObjectIdentifier(a->languagename);
	char	*langname2 = formatObjectIdentifier(b->languagename);

	if (strcmp(a->languagename, b->languagename) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER LANGUAGE %s RENAME TO %s;", langname1, langname2);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON LANGUAGE %s IS %s;", langname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON LANGUAGE %s IS NULL;", langname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS %s;",
						b->seclabels[i].provider,
						langname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS NULL;",
						a->seclabels[i].provider,
						langname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS %s;",
							b->seclabels[j].provider,
							langname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS NULL;",
							a->seclabels[i].provider,
							langname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS %s;",
								b->seclabels[j].provider,
								langname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS NULL;",
							a->seclabels[i].provider,
							langname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS %s;",
							b->seclabels[j].provider,
							langname2,
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
			fprintf(output, "ALTER LANGUAGE %s OWNER TO %s;",
					langname2,
					owner);

			free(owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		PQLObject tmpa, tmpb;

		tmpa.schemaname = NULL;
		tmpa.objectname = a->languagename;
		tmpb.schemaname = NULL;
		tmpb.objectname = b->languagename;

		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_LANGUAGE, &tmpa, &tmpb, a->acl, b->acl, NULL,
							   NULL);
	}

	free(langname1);
	free(langname2);
}
