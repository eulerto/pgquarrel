#include "language.h"

/*
 * CREATE LANGUAGE
 * DROP LANGUAGE
 * ALTER LANGUAGE
 * COMMENT ON LANGUAGE
 *
 * TODO
 *
 * ALTER LANGUAGE ... RENAME TO
 */

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
				 "SELECT l.oid, lanname AS languagename, CASE WHEN tmplname IS NULL THEN false ELSE true END AS pltemplate, lanpltrusted AS trusted, p1.proname AS callhandler, p2.proname AS inlinehandler, p3.proname AS validator, obj_description(l.oid, 'pg_language') AS description, pg_get_userbyid(lanowner) AS lanowner, lanacl FROM pg_language l LEFT JOIN pg_pltemplate t ON (l.lanname = t.tmplname) LEFT JOIN pg_proc p1 ON (p1.oid = lanplcallfoid) LEFT JOIN pg_proc p2 ON (p2.oid = laninline) LEFT JOIN pg_proc p3 ON (p3.oid = lanvalidator) WHERE lanispl AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE l.oid = d.objid AND d.deptype = 'e') ORDER BY lanname");
	}
	else
	{
		res = PQexec(c,
				 "SELECT l.oid, lanname AS languagename, CASE WHEN tmplname IS NULL THEN false ELSE true END AS pltemplate, lanpltrusted AS trusted, p1.proname AS callhandler, p2.proname AS inlinehandler, p3.proname AS validator, obj_description(l.oid, 'pg_language') AS description, pg_get_userbyid(lanowner) AS lanowner, lanacl FROM pg_language l LEFT JOIN pg_pltemplate t ON (l.lanname = t.tmplname) LEFT JOIN pg_proc p1 ON (p1.oid = lanplcallfoid) LEFT JOIN pg_proc p2 ON (p2.oid = laninline) LEFT JOIN pg_proc p3 ON (p3.oid = lanvalidator) WHERE lanispl ORDER BY lanname");
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
		l[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		l[i].languagename = strdup(PQgetvalue(res, i, PQfnumber(res, "languagename")));
		l[i].pltemplate = (PQgetvalue(res, i, PQfnumber(res, "pltemplate"))[0] == 't');
		l[i].trusted = (PQgetvalue(res, i, PQfnumber(res, "trusted"))[0] == 't');
		l[i].callhandler = strdup(PQgetvalue(res, i, PQfnumber(res, "callhandler")));
		l[i].inlinehandler = strdup(PQgetvalue(res, i, PQfnumber(res,
											   "inlinehandler")));
		l[i].validator = strdup(PQgetvalue(res, i, PQfnumber(res, "validator")));
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			l[i].comment = NULL;
		else
			l[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

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
	char		query[200];
	PGresult	*res;
	int			i;

	if (PG_VERSION_NUM < 90100)
	{
		logWarning("ignoring security labels because server does not support it");
		return;
	}

	snprintf(query, 200, "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_language' AND s.objoid = %u ORDER BY provider", l->oid);

	res = PQexec(c, query);

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

	logDebug("number of security labels in language \"%s\": %d", l->languagename, l->nseclabels);

	for (i = 0; i < l->nseclabels; i++)
	{
		l->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res, "provider")));
		l->seclabels[i].label = strdup(PQgetvalue(res, i, PQfnumber(res, "label")));
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
			free(l[i].callhandler);
			free(l[i].inlinehandler);
			free(l[i].validator);
			if (l[i].comment)
				free(l[i].comment);
			free(l[i].owner);
			if (l[i].acl)
				free(l[i].acl);

			/* security labels */
			for (j = 0; j < l[i].nseclabels; j++)
			{
				free(l[i].seclabels[j].provider);
				free(l[i].seclabels[j].label);
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

	fprintf(output, "\n\n");
	fprintf(output, "CREATE LANGUAGE %s", langname);

	if (!l->pltemplate)
	{
		if (l->trusted)
			fprintf(output, " TRUSTED");

		fprintf(output, " HANDLER %s", l->callhandler);
		fprintf(output, " INLINE %s", l->inlinehandler);
		fprintf(output, " VALIDATOR %s", l->validator);
	}

	fprintf(output, ";");

	/* comment */
	if (options.comment && l->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON LANGUAGE %s IS '%s';", langname, l->comment);
	}

	/* security labels */
	if (options.securitylabels && l->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < l->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS '%s';",
					l->seclabels[i].provider,
					langname,
					l->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER LANGUAGE %s OWNER TO %s;",
				langname,
				l->owner);
	}

	/* privileges */
	/* XXX second s.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
	{
		PQLObject tmp;

		tmp.schemaname = NULL;
		tmp.objectname = l->languagename;

		dumpGrantAndRevoke(output, PGQ_LANGUAGE, &tmp, &tmp, NULL, l->acl, NULL);
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
			fprintf(output, "COMMENT ON LANGUAGE %s IS '%s';", langname2, b->comment);
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
				fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS '%s';",
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
					fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS '%s';",
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
						fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS '%s';",
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
					fprintf(output, "SECURITY LABEL FOR %s ON LANGUAGE %s IS '%s';",
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
			fprintf(output, "\n\n");
			fprintf(output, "ALTER LANGUAGE %s OWNER TO %s;",
					langname2,
					b->owner);
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
			dumpGrantAndRevoke(output, PGQ_LANGUAGE, &tmpa, &tmpb, a->acl, b->acl, NULL);
	}

	free(langname1);
	free(langname2);
}
