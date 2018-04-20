/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * server.c
 *     Generate SERVER commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE SERVER
 * DROP SERVER
 * ALTER SERVER
 * COMMENT ON SERVER
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "server.h"


PQLForeignServer *
getForeignServers(PGconn *c, int *n)
{
	PQLForeignServer	*s;
	PGresult			*res;
	int					i;

	logNoise("foreign server: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90100)	/* extension support */
		res = PQexec(c,
					 "SELECT s.oid, s.srvname AS servername, s.srvowner AS owner, f.fdwname AS serverfdw, s.srvtype AS servertype, s.srvversion AS serverversion, array_to_string(s.srvoptions, ', ') AS options, obj_description(s.oid, 'pg_foreign_server') AS description, pg_get_userbyid(s.srvowner) AS serverowner, s.srvacl AS acl FROM pg_foreign_server s INNER JOIN pg_foreign_data_wrapper f ON (s.srvfdw = f.oid) WHERE NOT EXISTS(SELECT 1 FROM pg_depend d WHERE s.oid = d.objid AND d.deptype = 'e') ORDER BY srvname");
	else
		res = PQexec(c,
					 "SELECT s.oid, s.srvname AS servername, s.srvowner AS owner, f.fdwname AS serverfdw, s.srvtype AS servertype, s.srvversion AS serverversion, array_to_string(s.srvoptions, ', ') AS options, obj_description(s.oid, 'pg_foreign_server') AS description, pg_get_userbyid(s.srvowner) AS serverowner, s.srvacl AS acl FROM pg_foreign_server s INNER JOIN pg_foreign_data_wrapper f ON (s.srvfdw = f.oid) ORDER BY srvname");

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
		s = (PQLForeignServer *) malloc(*n * sizeof(PQLForeignServer));
	else
		s = NULL;

	logDebug("number of foreign servers in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		s[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		s[i].servername = strdup(PQgetvalue(res, i, PQfnumber(res, "servername")));
		s[i].serverfdw = strdup(PQgetvalue(res, i, PQfnumber(res, "serverfdw")));

		/* type (optional) */
		if (PQgetisnull(res, i, PQfnumber(res, "servertype")))
			s[i].servertype = NULL;
		else
			s[i].servertype = strdup(PQgetvalue(res, i, PQfnumber(res, "servertype")));

		/* version (optional) */
		if (PQgetisnull(res, i, PQfnumber(res, "serverversion")))
			s[i].serverversion = NULL;
		else
			s[i].serverversion = strdup(PQgetvalue(res, i, PQfnumber(res,
												   "serverversion")));

		/* options (optional) */
		if (PQgetisnull(res, i, PQfnumber(res, "options")))
			s[i].options = NULL;
		else
			s[i].options = strdup(PQgetvalue(res, i, PQfnumber(res, "options")));

		s[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "serverowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "acl")))
			s[i].acl = NULL;
		else
			s[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "acl")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			s[i].comment = NULL;
		else
			s[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("foreign server \"%s\"", s[i].servername);
	}

	PQclear(res);

	return s;
}

void
freeForeignServers(PQLForeignServer *s, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(s[i].servername);
			free(s[i].serverfdw);
			free(s[i].owner);
			if (s[i].servertype)
				free(s[i].servertype);
			if (s[i].serverversion)
				free(s[i].serverversion);
			if (s[i].options)
				free(s[i].options);
			if (s[i].acl)
				free(s[i].acl);
			if (s[i].comment)
				free(s[i].comment);
		}

		free(s);
	}
}

void
dumpDropForeignServer(FILE *output, PQLForeignServer *s)
{
	char	*srvname = formatObjectIdentifier(s->servername);

	fprintf(output, "\n\n");
	fprintf(output, "DROP SERVER %s;", srvname);

	free(srvname);
}

void
dumpCreateForeignServer(FILE *output, PQLForeignServer *s)
{
	char	*srvname = formatObjectIdentifier(s->servername);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE SERVER %s", srvname);

	/* type (optional) */
	if (s->servertype)
		fprintf(output, " TYPE '%s'", s->servertype);

	/* version (optional) */
	if (s->serverversion)
		fprintf(output, " VERSION '%s'", s->serverversion);

	/* foreign data wrapper */
	fprintf(output, " FOREIGN DATA WRAPPER %s", s->serverfdw);

	/* options (optional) */
	if (s->options)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(s->options);
		fprintf(output, " OPTIONS(");
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
				fprintf(output, ", %s '%s'", cell->value, str);
		}
		fprintf(output, ")");

		freeStringList(sl);
	}

	fprintf(output, ";");

	/* comment */
	if (options.comment && s->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON SERVER %s IS '%s';", srvname, s->comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SERVER %s OWNER TO %s;", srvname, s->owner);
	}

	/* privileges */
	/* XXX second tmp isn't used. Add an invalid PQLObject? */
	if (options.privileges)
	{
		PQLObject tmp;

		tmp.schemaname = NULL;
		tmp.objectname = s->servername;

		dumpGrantAndRevoke(output, PGQ_FOREIGN_SERVER, &tmp, &tmp, NULL, s->acl, NULL,
						   NULL);
	}

	free(srvname);
}

void
dumpAlterForeignServer(FILE *output, PQLForeignServer *a, PQLForeignServer *b)
{
	char	*srvname1 = formatObjectIdentifier(a->servername);
	char	*srvname2 = formatObjectIdentifier(b->servername);

	/* version */
	if (a->serverversion == NULL && b->serverversion != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SERVER %s VERSION '%s';", srvname2, b->serverversion);
	}
	else if (a->serverversion != NULL && b->serverversion == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SERVER %s VERSION NULL;", srvname2);
	}
	else if (a->serverversion != NULL && b->serverversion != NULL &&
			 strcmp(a->serverversion, b->serverversion) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SERVER %s VERSION '%s';", srvname2, b->serverversion);
	}

	/* options */
	if (a->options == NULL && b->options != NULL)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(b->options);
		fprintf(output, "\n\n");
		fprintf(output, "ALTER SERVER %s OPTIONS (", srvname2);
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
				fprintf(output, ", ADD %s '%s'", cell->value, str);
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
		fprintf(output, "ALTER SERVER %s OPTIONS (", srvname1);
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
				fprintf(output, ", DROP %s", cell->value);
		}
		fprintf(output, ");");

		freeStringList(sl);
	}
	else if (a->options != NULL && b->options != NULL &&
			 strcmp(a->options, b->options) != 0)
	{
		stringList	*rlist, *ilist, *slist;

		/* reset options that are only presented in the first set */
		rlist = setOperationOptions(a->options, b->options, PGQ_SETDIFFERENCE, false,
									true);
		if (rlist)
		{
			stringListCell	*cell;
			bool			first = true;

			fprintf(output, "\n\n");
			fprintf(output, "ALTER SERVER %s OPTIONS (", srvname2);
			for (cell = rlist->head; cell; cell = cell->next)
			{
				if (first)
				{
					fprintf(output, "DROP %s", cell->value);
					first = false;
				}
				else
					fprintf(output, ", DROP %s", cell->value);
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
			bool			first = true;

			fprintf(output, "\n\n");
			fprintf(output, "ALTER SERVER %s OPTIONS (", srvname2);
			for (cell = ilist->head; cell; cell = cell->next)
			{
				char	*str;

				str = strchr(cell->value, '=');
				*str++ = '\0';

				if (first)
				{
					fprintf(output, "SET %s '%s'", cell->value, str);
					first = false;
				}
				else
					fprintf(output, ", SET %s '%s'", cell->value, str);
			}
			fprintf(output, ");");

			freeStringList(ilist);
		}

		/*
		 * Set options that are only presented in the second set.
		 */
		slist = setOperationOptions(b->options, a->options, PGQ_SETDIFFERENCE, true,
									true);
		if (slist)
		{
			stringListCell	*cell;
			bool			first = true;

			fprintf(output, "\n\n");
			fprintf(output, "ALTER SERVER %s OPTIONS (", srvname2);
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
					fprintf(output, ", ADD %s '%s'", cell->value, str);
			}
			fprintf(output, ");");

			freeStringList(slist);
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
			fprintf(output, "COMMENT ON SERVER %s IS '%s';", srvname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON SERVER %s IS NULL;", srvname2);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER SERVER %s OWNER TO %s;", srvname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		PQLObject tmpa, tmpb;

		tmpa.schemaname = NULL;
		tmpa.objectname = a->servername;
		tmpb.schemaname = NULL;
		tmpb.objectname = b->servername;

		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_FOREIGN_SERVER, &tmpa, &tmpb, a->acl, b->acl,
							   NULL, NULL);
	}

	free(srvname1);
	free(srvname2);
}
