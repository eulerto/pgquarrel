/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * eventtrigger.c
 *     Generate EVENT TRIGGER commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE EVENT TRIGGER
 * DROP EVENT TRIGGER
 * ALTER EVENT TRIGGER
 * COMMENT ON EVENT TRIGGER
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "eventtrigger.h"


PQLEventTrigger *
getEventTriggers(PGconn *c, int *n)
{
	PQLEventTrigger	*e;
	PGresult	*res;
	int			i;

	logNoise("event trigger: server version: %d", PQserverVersion(c));

	/* bail out if we do not support it */
	if (PQserverVersion(c) < 90300)
	{
		logWarning("ignoring event triggers because server does not support it");
		return NULL;
	}

	res = PQexec(c,
				 "SELECT e.oid, e.evtname, e.evtevent, p.proname AS funcname, e.evtenabled, e.evttags, obj_description(e.oid, 'pg_event_trigger') AS description, pg_get_userbyid(e.evtowner) AS evtowner FROM pg_event_trigger e INNER JOIN pg_proc p ON (evtfoid = p.oid) WHERE NOT EXISTS(SELECT 1 FROM pg_depend d WHERE e.oid = d.objid AND d.deptype = 'e') ORDER BY evtname");

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
		e = (PQLEventTrigger *) malloc(*n * sizeof(PQLEventTrigger));
	else
		e = NULL;

	logDebug("number of event triggers in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		e[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		e[i].trgname = strdup(PQgetvalue(res, i, PQfnumber(res, "evtname")));
		e[i].event = strdup(PQgetvalue(res, i, PQfnumber(res, "evtevent")));
		if (PQgetisnull(res, i, PQfnumber(res, "evttags")))
			e[i].tags = NULL;
		else
			e[i].tags = strdup(PQgetvalue(res, i, PQfnumber(res, "evttags")));
		e[i].functionname = strdup(PQgetvalue(res, i, PQfnumber(res, "funcname")));
		e[i].enabled = PQgetvalue(res, i, PQfnumber(res, "evtenabled"))[0];
		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			e[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			e[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (e[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		e[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "evtowner")));

		/*
		 * Security labels are not assigned here (see
		 * getEventTriggerSecurityLabels), but default values are essential to
		 * avoid having trouble in freeEventTriggers.
		 */
		e[i].nseclabels = 0;
		e[i].seclabels = NULL;

		logDebug("event trigger \"%s\"", e[i].trgname);
	}

	PQclear(res);

	return e;
}

void
getEventTriggerSecurityLabels(PGconn *c, PQLEventTrigger *e)
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
			 "SELECT provider, label FROM pg_seclabel s INNER JOIN pg_class c ON (s.classoid = c.oid) WHERE c.relname = 'pg_event_trigger' AND s.objoid = %u ORDER BY provider",
			 e->oid);

	res = PQexec(c, query);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		logError("query failed: %s", PQresultErrorMessage(res));
		PQclear(res);
		PQfinish(c);
		/* XXX leak another connection? */
		exit(EXIT_FAILURE);
	}

	e->nseclabels = PQntuples(res);
	if (e->nseclabels > 0)
		e->seclabels = (PQLSecLabel *) malloc(e->nseclabels * sizeof(PQLSecLabel));
	else
		e->seclabels = NULL;

	logDebug("number of security labels in event trigger \"%s\": %d", e->trgname,
			 e->nseclabels);

	for (i = 0; i < e->nseclabels; i++)
	{
		char	*withoutescape;

		e->seclabels[i].provider = strdup(PQgetvalue(res, i, PQfnumber(res,
										  "provider")));
		withoutescape = PQgetvalue(res, i, PQfnumber(res, "label"));
		e->seclabels[i].label = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
		if (e->seclabels[i].label == NULL)
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
freeEventTriggers(PQLEventTrigger *e, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			int	j;

			free(e[i].trgname);
			free(e[i].event);
			if (e[i].tags)
				free(e[i].tags);
			free(e[i].functionname);
			if (e[i].comment)
				PQfreemem(e[i].comment);
			free(e[i].owner);

			/* security labels */
			for (j = 0; j < e[i].nseclabels; j++)
			{
				free(e[i].seclabels[j].provider);
				PQfreemem(e[i].seclabels[j].label);
			}

			if (e[i].seclabels)
				free(e[i].seclabels);
		}

		free(e);
	}
}

void
dumpCreateEventTrigger(FILE *output, PQLEventTrigger *e)
{
	char	*evtname = formatObjectIdentifier(e->trgname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE EVENT TRIGGER %s ON %s", evtname, e->event);

	if (e->tags != NULL)
		fprintf(output, "\n    WHEN TAG IN (%s)", e->tags);

	fprintf(output, "\n    EXECUTE PROCEDURE %s()", e->functionname);

	fprintf(output, ";");

	if (e->enabled != 'O')
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s", evtname);

		switch (e->enabled)
		{
			case 'D':
				fprintf(output, " DISABLE");
				break;
			case 'A':
				fprintf(output, " ENABLE ALWAYS");
				break;
			case 'R':
				fprintf(output, " ENABLE REPLICA");
				break;
			default:
				fprintf(output, " ENABLE");
				break;
		}

		fprintf(output, ";");
	}

	/* comment */
	if (options.comment && e->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON EVENT TRIGGER %s IS %s;", evtname, e->comment);
	}

	/* security labels */
	if (options.securitylabels && e->nseclabels > 0)
	{
		int	i;

		for (i = 0; i < e->nseclabels; i++)
		{
			fprintf(output, "\n\n");
			fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS %s;",
					e->seclabels[i].provider,
					evtname,
					e->seclabels[i].label);
		}
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s OWNER TO %s;",
				evtname,
				e->owner);
	}

	free(evtname);
}

void
dumpDropEventTrigger(FILE *output, PQLEventTrigger *e)
{
	char	*evtname = formatObjectIdentifier(e->trgname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP EVENT TRIGGER %s;", evtname);

	free(evtname);
}

void
dumpAlterEventTrigger(FILE *output, PQLEventTrigger *a, PQLEventTrigger *b)
{
	char	*evtname1 = formatObjectIdentifier(a->trgname);
	char	*evtname2 = formatObjectIdentifier(b->trgname);

	if (a->enabled != b->enabled)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s", evtname2);

		switch (b->enabled)
		{
			case 'D':
				fprintf(output, " DISABLE");
				break;
			case 'A':
				fprintf(output, " ENABLE ALWAYS");
				break;
			case 'R':
				fprintf(output, " ENABLE REPLICA");
				break;
			default:
				fprintf(output, " ENABLE");
				break;
		}

		fprintf(output, ";");
	}

	if (strcmp(a->trgname, b->trgname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s RENAME TO %s;", evtname1, evtname2);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EVENT TRIGGER %s IS %s;", evtname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EVENT TRIGGER %s IS NULL;", evtname2);
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
				fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS %s;",
						b->seclabels[i].provider,
						evtname2,
						b->seclabels[i].label);
			}
		}
		else if (a->seclabels != NULL && b->seclabels == NULL)
		{
			int	i;

			for (i = 0; i < a->nseclabels; i++)
			{
				fprintf(output, "\n\n");
				fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS NULL;",
						a->seclabels[i].provider,
						evtname1);
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
					fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS %s;",
							b->seclabels[j].provider,
							evtname2,
							b->seclabels[j].label);
					j++;
				}
				else if (j == b->nseclabels)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS NULL;",
							a->seclabels[i].provider, evtname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) == 0)
				{
					if (strcmp(a->seclabels[i].label, b->seclabels[j].label) != 0)
					{
						fprintf(output, "\n\n");
						fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS %s;",
								b->seclabels[j].provider,
								evtname2,
								b->seclabels[j].label);
					}
					i++;
					j++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) < 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS NULL;",
							a->seclabels[i].provider, evtname1);
					i++;
				}
				else if (strcmp(a->seclabels[i].provider, b->seclabels[j].provider) > 0)
				{
					fprintf(output, "\n\n");
					fprintf(output, "SECURITY LABEL FOR %s ON EVENT TRIGGER %s IS %s;",
							b->seclabels[j].provider,
							evtname2,
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
			fprintf(output, "ALTER EVENT TRIGGER %s OWNER TO %s;",
					evtname2,
					b->owner);
		}
	}

	free(evtname1);
	free(evtname2);
}
