/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database subscriptions
 *
 * subscription.c
 *     Generate SUBSCRIPTION commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE SUBSCRIPTION
 * DROP SUBSCRIPTION
 * ALTER SUBSCRIPTION
 * COMMENT ON SUBSCRIPTION
 *
 * TODO
 *
 * ALTER SUBSCRIPTION ... REFRESH PUBLICATION
 * ALTER SUBSCRIPTION ... RENAME TO
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "subscription.h"


static int comparePublications(PQLSubscription *a, PQLSubscription *b);

PQLSubscription *
getSubscriptions(PGconn *c, int *n)
{
	PQLSubscription	*s;
	PGresult		*res;
	int				i;

	logNoise("subscription: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 100000)
	{
		logWarning("ignoring subscriptions because server does not support it");
		return NULL;
	}

	res = PQexec(c,
				 "SELECT s.oid, subname, subenabled, subconninfo, subslotname, subsynccommit, obj_description(s.oid, 'pg_subscription') AS description, pg_get_userbyid(subowner) AS subowner FROM pg_subscription s ORDER BY subname");

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
		s = (PQLSubscription *) malloc(*n * sizeof(PQLSubscription));
	else
		s = NULL;

	logDebug("number of subscriptions in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		s[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		s[i].subname = strdup(PQgetvalue(res, i, PQfnumber(res, "subname")));
		s[i].conninfo = strdup(PQgetvalue(res, i, PQfnumber(res, "subconninfo")));
		if (PQgetisnull(res, i, PQfnumber(res, "subslotname")))
			s[i].slotname = NULL;
		else
			s[i].slotname = strdup(PQgetvalue(res, i, PQfnumber(res, "subslotname")));
		s[i].synccommit = strdup(PQgetvalue(res, i, PQfnumber(res, "subsynccommit")));
		s[i].enabled = (PQgetvalue(res, i, PQfnumber(res, "subenabled"))[0] == 't');
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			s[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			s[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (s[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		s[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "subowner")));

		/*
		 * These values are not assigned here (see getSubscriptionPublications), but
		 * defaults values are essential to avoid having trouble in
		 * freeSubscriptions.
		 */
		s[i].npublications = 0;
		s[i].publications = NULL;

		/*
		 * Security labels are not assigned here (see getSubscriptionSecurityLabels),
		 * but default values are essential to avoid having trouble in
		 * freeSubscriptions.
		 */
		s[i].nseclabels = 0;
		s[i].seclabels = NULL;

		logDebug("subscription \"%s\"", s[i].subname);
	}

	PQclear(res);

	return s;
}

void
getSubscriptionPublications(PGconn *c, PQLSubscription *s)
{
	char		*query = NULL;
	int			nquery = 0;
	PGresult	*res;
	int			i;

	/* determine how many characters will be written by snprintf */
	nquery = snprintf(query, nquery,
					  "SELECT unnest(subpublications) FROM pg_subscription s WHERE s.oid = %u ORDER BY 1",
					  s->oid);

	nquery++;
	query = (char *) malloc(nquery * sizeof(char));	/* make enough room for query */
	snprintf(query, nquery,
			 "SELECT unnest(subpublications) AS pubname FROM pg_subscription s WHERE s.oid = %u ORDER BY 1",
			 s->oid);

	logNoise("subscription: query size: %d ; query: %s", nquery, query);

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

	s->npublications = PQntuples(res);
	if (s->npublications > 0)
		s->publications = (PQLSubPublication *) malloc(s->npublications * sizeof(
							  PQLSubPublication));
	else
		s->publications = NULL;

	logDebug("number of publications in subscription \"%s\": %d", s->subname,
			 s->npublications);

	for (i = 0; i < s->npublications; i++)
	{
		s->publications[i].pubname = strdup(PQgetvalue(res, i, PQfnumber(res,
											"pubname")));

		logDebug("publication \"%s\" in subscription \"%s\"",
				 s->publications[i].pubname, s->subname);
	}

	PQclear(res);
}

void
getSubscriptionSecurityLabels(PGconn *c, PQLSubscription *s)
{
	char		query[200];
	PGresult	*res;
	int			i;

	snprintf(query, 200,
			 "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_subscription' AND s.objoid = %u ORDER BY provider",
			 s->oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	s->nseclabels = PQntuples(res);
	if (s->nseclabels > 0)
		s->seclabels = (PQLSecLabel *) malloc(s->nseclabels * sizeof(PQLSecLabel));
	else
		s->seclabels = NULL;

	logDebug("number of security labels in subscription \"%s\": %d", s->subname,
			 s->nseclabels);

	for (i = 0; i < s->nseclabels; i++)
	{
		char	*withoutescape;

		s->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		withoutescape = PQgetvalue(res, i, PQfnumber(res, "label"));
		s->seclabels[i].label = PQescapeLiteral(c, withoutescape,
												strlen(withoutescape));
		if (s->seclabels[i].label == NULL)
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
freeSubscriptions(PQLSubscription *s, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(s[i].subname);
			free(s[i].conninfo);
			if (s[i].slotname)
				free(s[i].slotname);
			free(s[i].synccommit);
			if (s[i].comment)
				PQfreemem(s[i].comment);
			free(s[i].owner);

			/* publications */
			for (j = 0; j < s[i].npublications; j++)
				free(s[i].publications[j].pubname);

			if (s[i].publications)
				free(s[i].publications);

			/* security labels */
			for (j = 0; j < s[i].nseclabels; j++)
			{
				free(s[i].seclabels[j].provider);
				PQfreemem(s[i].seclabels[j].label);
			}

			if (s[i].seclabels)
				free(s[i].seclabels);
		}

		free(s);
	}
}

void
dumpDropSubscription(FILE *output, PQLSubscription *s)
{
	char	*subscriptionname = formatObjectIdentifier(s->subname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP SUBSCRIPTION %s;", subscriptionname);

	free(subscriptionname);
}

void
dumpCreateSubscription(FILE *output, PQLSubscription *s)
{
	char	*subscriptionname = formatObjectIdentifier(s->subname);
	int		i;

	fprintf(output, "\n\n");
	fprintf(output, "CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION ",
			subscriptionname, s->conninfo);

	for (i = 0; i < s->npublications; i++)
	{
		char	*str = formatObjectIdentifier(s->publications[i].pubname);
		if (i > 0)
			fprintf(output, ", ");
		fprintf(output, "%s", str);
		free(str);
	}

	fprintf(output, " WITH (connect = false, slot_name = ");
	if (s->slotname)
		fprintf(output, "%s", s->slotname);
	else
		fprintf(output, "NONE");

	if (strcmp(s->synccommit, "off") != 0)
		fprintf(output, ", synchronous_commit = %s", s->synccommit);

	fprintf(output, ");");

	/* comment */
	if (options.comment && s->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON SUBSCRIPTION %s IS %s;", subscriptionname,
				s->comment);
	}

	/* security labels */
	if (options.securitylabels && s->nseclabels > 0)
	{
		for (i = 0; i < s->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS %s;",
					s->seclabels[i].provider,
					subscriptionname,
					s->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		char	*owner = formatObjectIdentifier(s->owner);

		fprintf(output, "\n\n");
		fprintf(output, "ALTER SUBSCRIPTION %s OWNER TO %s;", subscriptionname, owner);

		free(owner);
	}

	free(subscriptionname);
}

void
dumpAlterSubscription(FILE *output, PQLSubscription *a, PQLSubscription *b)
{
	char	*subscriptionname1 = formatObjectIdentifier(a->subname);
	char	*subscriptionname2 = formatObjectIdentifier(b->subname);
	int		i;

	/* publications */
	if (comparePublications(a, b) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SUBSCRIPTION %s SET PUBLICATION ", subscriptionname2);

		for (i = 0; i < b->npublications; i++)
		{
			char	*str = formatObjectIdentifier(b->publications[i].pubname);
			if (i > 0)
				fprintf(output, ", ");
			fprintf(output, "%s", str);
			free(str);
		}

		/*
		 * Let user executes ALTER SUBSCRIPTION foo REFRESH PUBLICATION
		 * separately because "refresh" can start some table synchronization.
		 * It seems an undesirable behavior in some situations.
		 */
		fprintf(output, " WITH (refresh = false);");
	}

	/* connection */
	if (strcmp(a->conninfo, b->conninfo) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SUBSCRIPTION %s CONNECTION '%s';", subscriptionname2,
				b->conninfo);
	}

	/* enable / disable */
	if ((a->enabled && !b->enabled) || (!a->enabled && b->enabled))
	{
		fprintf(output, "\n\n");
		if (b->enabled)
			fprintf(output, "ALTER SUBSCRIPTION %s ENABLE;", subscriptionname2);
		else
			fprintf(output, "ALTER SUBSCRIPTION %s DISABLE;", subscriptionname2);
	}

	/* options */
	if ((a->synccommit && !b->synccommit) || (!a->synccommit && b->synccommit))
	{
		fprintf(output, "\n\n");
		if (b->synccommit)
			fprintf(output, "ALTER SUBSCRIPTION %s SET (synchronous_commit = on);",
					subscriptionname2);
		else
			fprintf(output, "ALTER SUBSCRIPTION %s SET (synchronous_commit = off);",
					subscriptionname2);
	}

	if ((a->slotname == NULL && b->slotname != NULL) ||
			(a->slotname != NULL && b->slotname != NULL &&
			 strcmp(a->slotname, b->slotname) != 0))
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SUBSCRIPTION %s SET (slot_name = %s);",
				subscriptionname2, b->slotname);
	}
	else if (a->slotname != NULL && b->slotname == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SUBSCRIPTION %s SET (slot_name = NONE);",
				subscriptionname2);
	}


	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SUBSCRIPTION %s IS %s;", subscriptionname2,
					b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SUBSCRIPTION %s IS NULL;", subscriptionname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS %s;",
						b->seclabels[i].provider,
						subscriptionname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS NULL;",
						a->seclabels[i].provider,
						subscriptionname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS %s;",
							b->seclabels[j].provider,
							subscriptionname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS NULL;",
							a->seclabels[i].provider,
							subscriptionname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS %s;",
								b->seclabels[j].provider,
								subscriptionname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS NULL;",
							a->seclabels[i].provider,
							subscriptionname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON SUBSCRIPTION %s IS %s;",
							b->seclabels[j].provider,
							subscriptionname2,
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
			fprintf(output, "ALTER SUBSCRIPTION %s OWNER TO %s;", subscriptionname2, owner);

			free(owner);
		}
	}

	free(subscriptionname1);
	free(subscriptionname2);
}

static int
comparePublications(PQLSubscription *a, PQLSubscription *b)
{
	int		i;

	/* different number of elements, bail out */
	if (a->npublications != b->npublications)
		return 1;

	/*
	 * According to getSubscriptionPublications, publication names are sorted by
	 * name, hence, we can compare elements using the same index.
	 */
	for (i = 0; i < a->npublications; i++)
		if (strcmp(a->publications[i].pubname, b->publications[i].pubname) != 0)
			return 1;

	return 0;
}
