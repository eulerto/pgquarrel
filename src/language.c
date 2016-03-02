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

	res = PQexec(c,
				 "SELECT l.oid, lanname AS languagename, CASE WHEN tmplname IS NULL THEN false ELSE true END AS pltemplate, lanpltrusted AS trusted, p1.proname AS callhandler, p2.proname AS inlinehandler, p3.proname AS validator, d.description, pg_get_userbyid(lanowner) AS lanowner, lanacl FROM pg_language l LEFT JOIN pg_pltemplate t ON (l.lanname = t.tmplname) LEFT JOIN pg_proc p1 ON (p1.oid = lanplcallfoid) LEFT JOIN pg_proc p2 ON (p2.oid = laninline) LEFT JOIN pg_proc p3 ON (p3.oid = lanvalidator) LEFT JOIN (pg_description d INNER JOIN pg_class x ON (x.oid = d.classoid AND x.relname = 'pg_language')) ON (d.objoid = l.oid) WHERE lanispl ORDER BY lanname");

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

		logDebug("language %s", formatObjectIdentifier(l[i].languagename));
	}

	PQclear(res);

	return l;
}

void
freeLanguages(PQLLanguage *l, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(l[i].languagename);
			free(l[i].callhandler);
			free(l[i].inlinehandler);
			free(l[i].validator);
			if (l[i].comment)
				free(l[i].comment);
			free(l[i].owner);
			if (l[i].acl)
				free(l[i].acl);
		}

		free(l);
	}
}

void
dumpDropLanguage(FILE *output, PQLLanguage l)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP LANGUAGE %s;",
			formatObjectIdentifier(l.languagename));
}

void
dumpCreateLanguage(FILE *output, PQLLanguage l)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE LANGUAGE %s",
			formatObjectIdentifier(l.languagename));

	if (!l.pltemplate)
	{
		if (l.trusted)
			fprintf(output, " TRUSTED");

		fprintf(output, " HANDLER %s", l.callhandler);
		fprintf(output, " INLINE %s", l.inlinehandler);
		fprintf(output, " VALIDATOR %s", l.validator);
	}

	fprintf(output, ";");

	/* comment */
	if (options.comment && l.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON LANGUAGE %s IS '%s';",
				formatObjectIdentifier(l.languagename), l.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER LANGUAGE %s OWNER TO %s;",
				formatObjectIdentifier(l.languagename),
				l.owner);
	}

	/* privileges */
	/* XXX second s.obj isn't used. Add an invalid PQLObject? */
	if (options.privileges)
	{
		PQLObject tmp;

		tmp.schemaname = NULL;
		tmp.objectname = l.languagename;

		dumpGrantAndRevoke(output, PGQ_LANGUAGE, tmp, tmp, NULL, l.acl, NULL);
	}
}

void
dumpAlterLanguage(FILE *output, PQLLanguage a, PQLLanguage b)
{
	if (strcmp(a.languagename, b.languagename) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER LANGUAGE %s RENAME TO %s;",
				formatObjectIdentifier(a.languagename),
				formatObjectIdentifier(b.languagename));
	}

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON LANGUAGE %s IS '%s';",
					formatObjectIdentifier(b.languagename), b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON LANGUAGE %s IS NULL;",
					formatObjectIdentifier(b.languagename));
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a.owner, b.owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER LANGUAGE %s OWNER TO %s;",
					formatObjectIdentifier(b.languagename),
					b.owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		PQLObject tmpa, tmpb;

		tmpa.schemaname = NULL;
		tmpa.objectname = a.languagename;
		tmpb.schemaname = NULL;
		tmpb.objectname = b.languagename;

		if (a.acl != NULL || b.acl != NULL)
			dumpGrantAndRevoke(output, PGQ_LANGUAGE, tmpa, tmpb, a.acl, b.acl, NULL);
	}
}
