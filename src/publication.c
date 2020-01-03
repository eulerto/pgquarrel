/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database publications
 *
 * publication.c
 *     Generate PUBLICATION commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE PUBLICATION
 * DROP PUBLICATION
 * ALTER PUBLICATION
 * COMMENT ON PUBLICATION
 *
 * TODO
 *
 * ALTER PUBLICATION ... RENAME TO
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "publication.h"


static void dumpAddTable(FILE *output, PQLPublication *p, int i);
static void dumpRemoveTable(FILE *output, PQLPublication *p, int i);

PQLPublication *
getPublications(PGconn *c, int *n)
{
	PQLPublication	*p;
	PGresult	*res;
	int			i;

	logNoise("publication: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 100000)
	{
		logWarning("ignoring publications because server does not support it");
		return NULL;
	}

	if (PQserverVersion(c) >= 110000)
	{
		res = PQexec(c,
					 "SELECT p.oid, pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, obj_description(p.oid, 'pg_publication') AS description, pg_get_userbyid(pubowner) AS pubowner FROM pg_publication p ORDER BY pubname");
	}
	else if (PQserverVersion(c) >= 100000)
	{
		res = PQexec(c,
					 "SELECT p.oid, pubname, puballtables, pubinsert, pubupdate, pubdelete, false AS pubtruncate, obj_description(p.oid, 'pg_publication') AS description, pg_get_userbyid(pubowner) AS pubowner FROM pg_publication p ORDER BY pubname");
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
		p = (PQLPublication *) malloc(*n * sizeof(PQLPublication));
	else
		p = NULL;

	logDebug("number of publications in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		p[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		p[i].pubname = strdup(PQgetvalue(res, i, PQfnumber(res, "pubname")));
		p[i].alltables = (PQgetvalue(res, i, PQfnumber(res, "puballtables"))[0] == 't');
		p[i].pubinsert = (PQgetvalue(res, i, PQfnumber(res, "pubinsert"))[0] == 't');
		p[i].pubupdate = (PQgetvalue(res, i, PQfnumber(res, "pubupdate"))[0] == 't');
		p[i].pubdelete = (PQgetvalue(res, i, PQfnumber(res, "pubdelete"))[0] == 't');
		p[i].pubtruncate = (PQgetvalue(res, i, PQfnumber(res,
									   "pubtruncate"))[0] == 't');
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			p[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			p[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (p[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		p[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "pubowner")));

		/*
		 * These values are not assigned here (see getPublicationTables), but
		 * defaults values are essential to avoid having trouble in
		 * freePublications.
		 */
		p[i].ntables = 0;
		p[i].tables = NULL;

		/*
		 * Security labels are not assigned here (see getPublicationSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freePublications.
		 */
		p[i].nseclabels = 0;
		p[i].seclabels = NULL;

		logDebug("publication \"%s\"", p[i].pubname);
	}

	PQclear(res);

	return p;
}

void
getPublicationTables(PGconn *c, PQLPublication *p)
{
	char		*query = NULL;
	int			nquery = 0;
	PGresult	*res;
	int			i;

	/* determine how many characters will be written by snprintf */
	nquery = snprintf(query, nquery,
					  "SELECT n.nspname, c.relname FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) INNER JOIN pg_publication_rel pr ON (c.oid = pr.prrelid) WHERE pr.prpubid = %u ORDER BY n.nspname, c.relname",
					  p->oid);

	nquery++;
	query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
	snprintf(query, nquery,
			 "SELECT n.nspname, c.relname FROM pg_class c INNER JOIN pg_namespace n ON (c.relnamespace = n.oid) INNER JOIN pg_publication_rel pr ON (c.oid = pr.prrelid) WHERE pr.prpubid = %u ORDER BY n.nspname, c.relname",
			 p->oid);

	logNoise("publication: query size: %d ; query: %s", nquery, query);

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

	p->ntables = PQntuples(res);
	if (p->ntables > 0)
		p->tables = (PQLObject *) malloc(p->ntables * sizeof(PQLObject));
	else
		p->tables = NULL;

	logDebug("number of tables in publication \"%s\": %d", p->pubname, p->ntables);

	for (i = 0; i < p->ntables; i++)
	{
		p->tables[i].schemaname = strdup(PQgetvalue(res, i, PQfnumber(res,
										 "nspname")));
		p->tables[i].objectname = strdup(PQgetvalue(res, i, PQfnumber(res,
										 "relname")));

		logDebug("table \"%s\".\"%s\" in publication \"%s\"", p->tables[i].schemaname,
				 p->tables[i].objectname, p->pubname);
	}

	PQclear(res);
}

void
getPublicationSecurityLabels(PGconn *c, PQLPublication *p)
{
	char		query[200];
	PGresult	*res;
	int			i;

	snprintf(query, 200,
			 "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_publication' AND s.objoid = %u ORDER BY provider",
			 p->oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	p->nseclabels = PQntuples(res);
	if (p->nseclabels > 0)
		p->seclabels = (PQLSecLabel *) malloc(p->nseclabels * sizeof(PQLSecLabel));
	else
		p->seclabels = NULL;

	logDebug("number of security labels in publication \"%s\": %d", p->pubname,
			 p->nseclabels);

	for (i = 0; i < p->nseclabels; i++)
	{
		char	*withoutescape;

		p->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		withoutescape = PQgetvalue(res, i, PQfnumber(res, "label"));
		p->seclabels[i].label = PQescapeLiteral(c, withoutescape,
												strlen(withoutescape));
		if (p->seclabels[i].label == NULL)
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
freePublications(PQLPublication *p, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(p[i].pubname);
			if (p[i].comment)
				PQfreemem(p[i].comment);
			free(p[i].owner);

			/* tables */
			for (j = 0; j < p[i].ntables; j++)
			{
				free(p[i].tables[j].schemaname);
				free(p[i].tables[j].objectname);
			}

			if (p[i].tables)
				free(p[i].tables);

			/* security labels */
			for (j = 0; j < p[i].nseclabels; j++)
			{
				free(p[i].seclabels[j].provider);
				PQfreemem(p[i].seclabels[j].label);
			}

			if (p[i].seclabels)
				free(p[i].seclabels);
		}

		free(p);
	}
}

void
dumpDropPublication(FILE *output, PQLPublication *p)
{
	char	*publicationname = formatObjectIdentifier(p->pubname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP PUBLICATION %s;", publicationname);

	free(publicationname);
}

void
dumpCreatePublication(FILE *output, PQLPublication *p)
{
	char	*publicationname = formatObjectIdentifier(p->pubname);
	bool	first = true;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE PUBLICATION %s", publicationname);

	if (p->alltables)
		fprintf(output, " FOR ALL TABLES");

	fprintf(output, " WITH (publish = '");
	if (p->pubinsert)
	{
		fprintf(output, "insert");
		first = false;
	}
	if (p->pubupdate)
	{
		if (!first)
			fprintf(output, ", ");
		fprintf(output, "update");
		first = false;
	}
	if (p->pubdelete)
	{
		if (!first)
			fprintf(output, ", ");
		fprintf(output, "delete");
		first = false;
	}
	if (p->pubtruncate)
	{
		if (!first)
			fprintf(output, ", ");
		fprintf(output, "truncate");
		first = false;
	}
	fprintf(output, "');");

	if (p->ntables > 0)
	{
		int	i;

		for (i = 0; i < p->ntables; i++)
		{
			char	*schemaname = formatObjectIdentifier(p->tables[i].schemaname);
			char	*tablename = formatObjectIdentifier(p->tables[i].objectname);
			fprintf(output, "\n\n");
			fprintf(output, "ALTER PUBLICATION %s ADD TABLE ONLY %s.%s;", publicationname,
					schemaname, tablename);
			free(schemaname);
			free(tablename);
		}
	}

	/* comment */
	if (options.comment && p->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON PUBLICATION %s IS %s;", publicationname,
				p->comment);
	}

	/* security labels */
	if (options.securitylabels && p->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < p->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS %s;",
					p->seclabels[i].provider,
					publicationname,
					p->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(p->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER PUBLICATION %s OWNER TO %s;", publicationname, owner);

		free(owner);
	}

	free(publicationname);
}

void
dumpAlterPublication(FILE *output, PQLPublication *a, PQLPublication *b)
{
	char	*publicationname1 = formatObjectIdentifier(a->pubname);
	char	*publicationname2 = formatObjectIdentifier(b->pubname);
	int		i, j;

	/* tables in publication are sorted by schema and table */
	i = j = 0;
	while (i < a->ntables || j < b->ntables)
	{
		/*
		 * End of publication a tables. Additional tables from publication b
		 * will be added.
		 */
		if (i == a->ntables)
		{
			logDebug("publication \"%s\" table \"%s\".\"%s\" added",
					 b->pubname,
					 b->tables[j].schemaname, b->tables[j].objectname);

			dumpAddTable(output, b, j);

			j++;
		}
		/*
		 * End of publication b tables. Additional tables from publication a
		 * will be removed.
		 */
		else if (j == b->ntables)
		{
			logDebug("publication \"%s\" table \"%s\".\"%s\" removed",
					 a->pubname,
					 a->tables[i].schemaname, a->tables[i].objectname);

			dumpRemoveTable(output, a, i);
			i++;
		}
		else if (compareRelations(&a->tables[i], &b->tables[j]) == 0)
		{
			i++;
			j++;
		}
		else if (compareRelations(&a->tables[i], &b->tables[j]) < 0)
		{
			logDebug("publication \"%s\" table \"%s\".\"%s\" removed",
					 a->pubname,
					 a->tables[i].schemaname, a->tables[i].objectname);

			dumpRemoveTable(output, a, i);
			i++;
		}
		else if (compareRelations(&a->tables[i], &b->tables[j]) > 0)
		{
			logDebug("publication \"%s\" table \"%s\".\"%s\" added",
					 b->pubname,
					 b->tables[j].schemaname, b->tables[j].objectname);

			dumpAddTable(output, b, j);

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
			fprintf(output, "COMMENT ON PUBLICATION %s IS %s;", publicationname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON PUBLICATION %s IS NULL;", publicationname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS %s;",
						b->seclabels[i].provider,
						publicationname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS NULL;",
						a->seclabels[i].provider,
						publicationname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS %s;",
							b->seclabels[j].provider,
							publicationname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS NULL;",
							a->seclabels[i].provider,
							publicationname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS %s;",
								b->seclabels[j].provider,
								publicationname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS NULL;",
							a->seclabels[i].provider,
							publicationname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON PUBLICATION %s IS %s;",
							b->seclabels[j].provider,
							publicationname2,
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
			fprintf(output, "ALTER PUBLICATION %s OWNER TO %s;", publicationname2, owner);

			free(owner);
		}
	}

	free(publicationname1);
	free(publicationname2);
}

static void
dumpAddTable(FILE *output, PQLPublication *p, int i)
{
	char	*publicationname = formatObjectIdentifier(p->pubname);
	char	*schemaname = formatObjectIdentifier(p->tables[i].schemaname);
	char	*tablename = formatObjectIdentifier(p->tables[i].objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER PUBLICATION %s ADD TABLE ONLY %s.%s;", publicationname,
			schemaname, tablename);

	free(publicationname);
	free(schemaname);
	free(tablename);
}

static void
dumpRemoveTable(FILE *output, PQLPublication *p, int i)
{
	char	*publicationname = formatObjectIdentifier(p->pubname);
	char	*schemaname = formatObjectIdentifier(p->tables[i].schemaname);
	char	*tablename = formatObjectIdentifier(p->tables[i].objectname);

	fprintf(output, "\n\n");
	fprintf(output, "ALTER PUBLICATION %s DROP TABLE ONLY %s.%s;", publicationname,
			schemaname, tablename);

	free(publicationname);
	free(schemaname);
	free(tablename);
}
