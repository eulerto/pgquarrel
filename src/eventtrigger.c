#include "eventtrigger.h"

/*
 * CREATE EVENT TRIGGER
 * DROP EVENT TRIGGER
 * ALTER EVENT TRIGGER
 * COMMENT ON EVENT TRIGGER
 */

PQLEventTrigger *
getEventTriggers(PGconn *c, int *n)
{
	PQLEventTrigger	*e;
	PGresult	*res;
	int			i;

	logNoise("event trigger: server version: %d", PQserverVersion(c));

	res = PQexec(c,
				 "SELECT e.evtname, e.evtevent, p.proname AS funcname, e.evtenabled, e.evttags, obj_description(e.oid, 'pg_event_trigger') AS description, pg_get_userbyid(e.evtowner) AS evtowner FROM pg_event_trigger e INNER JOIN pg_proc p ON (evtfoid = p.oid) ORDER BY evtname");

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
			e[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		e[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "evtowner")));

		logDebug("event trigger %s", formatObjectIdentifier(e[i].trgname));
	}

	PQclear(res);

	return e;
}

void
dumpCreateEventTrigger(FILE *output, PQLEventTrigger e)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE EVENT TRIGGER %s ON %s", formatObjectIdentifier(e.trgname), e.event);

	if (e.tags != NULL)
		fprintf(output, "\n    WHEN TAG IN (%s)", e.tags);

	fprintf(output, "\n    EXECUTE PROCEDURE %s()", e.functionname);

	fprintf(output, ";");

	if (e.enabled != 'O')
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s", formatObjectIdentifier(e.trgname));

		switch (e.enabled)
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
	if (options.comment && e.comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON EVENT TRIGGER %s IS '%s';",
				formatObjectIdentifier(e.trgname),
				e.comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s OWNER TO %s;",
				formatObjectIdentifier(e.trgname),
				e.owner);
	}
}

void
dumpDropEventTrigger(FILE *output, PQLEventTrigger e)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP EVENT TRIGGER %s;", e.trgname);
}

void
dumpAlterEventTrigger(FILE *output, PQLEventTrigger a, PQLEventTrigger b)
{
	if (a.enabled != b.enabled)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s", b.trgname);

		switch (b.enabled)
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

	if (strcmp(a.trgname, b.trgname) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER EVENT TRIGGER %s RENAME TO %s;", a.trgname, b.trgname);
	}

	/* comment */
	if (options.comment)
	{
		if ((a.comment == NULL && b.comment != NULL) ||
				(a.comment != NULL && b.comment != NULL &&
				 strcmp(a.comment, b.comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EVENT TRIGGER %s IS '%s';",
					formatObjectIdentifier(b.trgname),
					b.comment);
		}
		else if (a.comment != NULL && b.comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON EVENT TRIGGER %s IS NULL;",
					formatObjectIdentifier(b.trgname));
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a.owner, b.owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER EVENT TRIGGER %s OWNER TO %s;",
					formatObjectIdentifier(b.trgname),
					b.owner);
		}
	}
}
