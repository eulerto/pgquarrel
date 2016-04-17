#include "usermapping.h"

/*
 * CREATE USER MAPPING
 * DROP USER MAPPING
 * ALTER USER MAPPING
 */

int
compareUserMappings(PQLUserMapping *a, PQLUserMapping *b)
{
	int		c;

	c = strcmp(a->user, b->user);

	/* compare server names iif user names are equal */
	if (c == 0)
		c = strcmp(a->server, b->server);

	return c;
}

PQLUserMapping *
getUserMappings(PGconn *c, int *n)
{
	PQLUserMapping	*u;
	PGresult		*res;
	int				i;

	logNoise("user mapping: server version: %d", PQserverVersion(c));

	res = PQexec(c, "SELECT u.oid, u.umuser AS useroid, CASE WHEN umuser = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(u.umuser) END AS username, s.srvname AS servername, array_to_string(u.umoptions, ', ') AS options FROM pg_user_mapping u INNER JOIN pg_foreign_server s ON (u.umserver = s.oid) ORDER BY username, servername");

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
		u = (PQLUserMapping *) malloc(*n * sizeof(PQLUserMapping));
	else
		u = NULL;

	logDebug("number of user mappings in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		u[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		u[i].useroid = strtoul(PQgetvalue(res, i, PQfnumber(res, "useroid")), NULL, 10);
		u[i].user = strdup(PQgetvalue(res, i, PQfnumber(res, "username")));
		u[i].server = strdup(PQgetvalue(res, i, PQfnumber(res, "servername")));

		/* options (optional) */
		if (PQgetisnull(res, i, PQfnumber(res, "options")))
			u[i].options = NULL;
		else
			u[i].options = strdup(PQgetvalue(res, i, PQfnumber(res, "options")));

		logDebug("user mapping for user %s server %s", u[i].user, u[i].server);
	}

	PQclear(res);

	return u;
}

void
freeUserMappings(PQLUserMapping *u, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(u[i].user);
			free(u[i].server);
			if (u[i].options)
				free(u[i].options);
		}

		free(u);
	}
}

void
dumpDropUserMapping(FILE *output, PQLUserMapping *u)
{
	fprintf(output, "\n\n");
	fprintf(output, "DROP USER MAPPING FOR %s SERVER %s;", u->user, u->server);
}

void
dumpCreateUserMapping(FILE *output, PQLUserMapping *u)
{
	fprintf(output, "\n\n");
	fprintf(output, "CREATE USER MAPPING FOR %s SERVER %s", u->user, u->server);

	/* options (optional) */
	if (u->options)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(u->options);
		fprintf(output, " OPTIONS (");
		for (cell = sl->head; cell; cell = cell->next)
		{
			char	*str;

			str = strchr(cell->value, '=');
			*str++ = '\0';

			if (first)
			{
				fprintf(output, "%s '%s'", cell->value, str);
				first = false;
			}
			else
			{
				fprintf(output, ", %s '%s'", cell->value, str);
			}
		}
		fprintf(output, ")");

		freeStringList(sl);
	}

	fprintf(output, ";");
}

void
dumpAlterUserMapping(FILE *output, PQLUserMapping *a, PQLUserMapping *b)
{
	/* options */
	if (a->options == NULL && b->options != NULL)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(b->options);
		fprintf(output, "\n\n");
		fprintf(output, "ALTER USER MAPPING FOR %s SERVER %s OPTIONS (", b->user, b->server);
		for (cell = sl->head; cell; cell = cell->next)
		{
			char	*str;

			str = strchr(cell->value, '=');
			*str++ = '\0';

			if (first)
			{
				fprintf(output, "ADD %s '%s'", cell->value, str);
				first = false;
			}
			else
			{
				fprintf(output, ", ADD %s '%s'", cell->value, str);
			}
		}
		fprintf(output, ");");

		freeStringList(sl);
	}
	else if (a->options != NULL && b->options == NULL)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(a->options);
		fprintf(output, "\n\n");
		fprintf(output, "ALTER USER MAPPING FOR %s SERVER %s OPTIONS (", a->user, a->server);
		for (cell = sl->head; cell; cell = cell->next)
		{
			char	*str;

			str = strchr(cell->value, '=');
			*str = '\0';

			if (first)
			{
				fprintf(output, "DROP %s", cell->value);
				first = false;
			}
			else
			{
				fprintf(output, ", DROP %s", cell->value);
			}
		}
		fprintf(output, ");");

		freeStringList(sl);
	}
	else if (a->options != NULL && b->options != NULL && strcmp(a->options, b->options) != 0)
	{
		stringList	*rlist, *ilist, *slist;
		bool		first = true;

		/* reset options that are only presented in the first set */
		rlist = setOperationOptions(a->options, b->options, PGQ_SETDIFFERENCE, false, true);
		if (rlist)
		{
			stringListCell	*cell;

			fprintf(output, "\n\n");
			fprintf(output, "ALTER USER MAPPING FOR %s SERVER %s OPTIONS (", a->user, a->server);
			for (cell = rlist->head; cell; cell = cell->next)
			{
				if (first)
				{
					fprintf(output, " DROP %s", cell->value);
					first = false;
				}
				else
				{
					fprintf(output, ", DROP %s", cell->value);
				}
			}
			fprintf(output, ");");

			freeStringList(rlist);
		}

		/*
		 * Include intersection between option sets. However, exclude options
		 * that don't change.
		 */
		ilist = setOperationOptions(a->options, b->options, PGQ_INTERSECT, true, true);
		if (ilist)
		{
			stringListCell	*cell;

			fprintf(output, "\n\n");
			fprintf(output, "ALTER USER MAPPING FOR %s SERVER %s OPTIONS (", a->user, a->server);
			for (cell = ilist->head; cell; cell = cell->next)
			{
				char	*str;

				str = strchr(cell->value, '=');
				*str++ = '\0';

				if (first)
				{
					fprintf(output, " SET %s '%s'", cell->value, str);
					first = false;
				}
				else
				{
					fprintf(output, ", SET %s '%s'", cell->value, str);
				}
			}
			fprintf(output, ");");

			freeStringList(ilist);
		}

		/*
		 * Set options that are only presented in the second set.
		 */
		slist = setOperationOptions(b->options, a->options, PGQ_SETDIFFERENCE, true, true);
		if (slist)
		{
			stringListCell	*cell;
			bool			first = true;

			fprintf(output, "\n\n");
			fprintf(output, "ALTER USER MAPPING FOR %s SERVER %s OPTIONS (", a->user, a->server);
			for (cell = slist->head; cell; cell = cell->next)
			{
				char	*str;

				str = strchr(cell->value, '=');
				*str++ = '\0';

				if (first)
				{
					fprintf(output, "ADD %s '%s'", cell->value, str);
					first = false;
				}
				else
				{
					fprintf(output, ", ADD %s '%s'", cell->value, str);
				}
			}
			fprintf(output, ");");

			freeStringList(slist);
		}
	}
}
