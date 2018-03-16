/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * fdw.c
 *     Generate FOREIGN DATA WRAPPER commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE FOREIGN DATA WRAPPER
 * DROP FOREIGN DATA WRAPPER
 * ALTER FOREIGN DATA WRAPPER
 * COMMENT ON FOREIGN DATA WRAPPER
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "fdw.h"


PQLForeignDataWrapper *
getForeignDataWrappers(PGconn *c, int *n)
{
	PQLForeignDataWrapper	*f;
	PGresult				*res;
	int						i;

	logNoise("fdw: server version: %d", PQserverVersion(c));

	if (PQserverVersion(c) >= 90100)	/* extension support */
	{
		res = PQexec(c, "SELECT f.oid, f.fdwname, f.fdwhandler, f.fdwvalidator, m.nspname AS handlernspname, h.oid AS handleroid, h.proname AS handlername, n.nspname AS validatornspname, v.oid AS validatoroid, v.proname AS validatorname, array_to_string(f.fdwoptions, ', ') AS options, obj_description(f.oid, 'pg_foreign_data_wrapper') AS description, pg_get_userbyid(f.fdwowner) AS fdwowner, f.fdwacl FROM pg_foreign_data_wrapper f LEFT JOIN (pg_proc h INNER JOIN pg_namespace m ON (m.oid = h.pronamespace)) ON (h.oid = f.fdwhandler) LEFT JOIN (pg_proc v INNER JOIN pg_namespace n ON (n.oid = v.pronamespace)) ON (v.oid = f.fdwvalidator) WHERE NOT EXISTS(SELECT 1 FROM pg_depend d WHERE f.oid = d.objid AND d.deptype = 'e') ORDER BY fdwname");
	}
	else
	{
		res = PQexec(c, "SELECT f.oid, f.fdwname, 0 AS fdwhandler, f.fdwvalidator, NULL AS handlernspname, 0 AS handleroid, NULL AS handlername, n.nspname AS validatornspname, v.oid AS validatoroid, v.proname AS validatorname, array_to_string(f.fdwoptions, ', ') AS options, obj_description(f.oid, 'pg_foreign_data_wrapper') AS description, pg_get_userbyid(f.fdwowner) AS fdwowner, f.fdwacl FROM pg_foreign_data_wrapper f LEFT JOIN (pg_proc v INNER JOIN pg_namespace n ON (n.oid = v.pronamespace)) ON (v.oid = f.fdwvalidator) ORDER BY fdwname");
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
		f = (PQLForeignDataWrapper *) malloc(*n * sizeof(PQLForeignDataWrapper));
	else
		f = NULL;

	logDebug("number of foreign data wrappers in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		f[i].oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		f[i].fdwname = strdup(PQgetvalue(res, i, PQfnumber(res, "fdwname")));
		/* handler */
		if (strcmp(PQgetvalue(res, i, PQfnumber(res, "fdwhandler")), "0") != 0)
		{
			f[i].handler.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "handleroid")), NULL, 10);
			f[i].handler.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "handlernspname")));
			f[i].handler.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "handlername")));
		}
		else
		{
			f[i].handler.schemaname = NULL;
			f[i].handler.objectname = NULL;
		}
		/* validator */
		if (strcmp(PQgetvalue(res, i, PQfnumber(res, "fdwvalidator")), "0") != 0)
		{
			f[i].validator.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "validatoroid")), NULL, 10);
			f[i].validator.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "validatornspname")));
			f[i].validator.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "validatorname")));
		}
		else
		{
			f[i].validator.schemaname = NULL;
			f[i].validator.objectname = NULL;
		}

		if (PQgetisnull(res, i, PQfnumber(res, "options")))
			f[i].options = NULL;
		else
			f[i].options = strdup(PQgetvalue(res, i, PQfnumber(res, "options")));

		f[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "fdwowner")));
		if (PQgetisnull(res, i, PQfnumber(res, "fdwacl")))
			f[i].acl = NULL;
		else
			f[i].acl = strdup(PQgetvalue(res, i, PQfnumber(res, "fdwacl")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			f[i].comment = NULL;
		else
			f[i].comment = strdup(PQgetvalue(res, i, PQfnumber(res, "description")));

		logDebug("foreign data wrapper \"%s\"", f[i].fdwname);
	}

	PQclear(res);

	return f;
}

void
freeForeignDataWrappers(PQLForeignDataWrapper *f, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(f[i].fdwname);
			if (f[i].handler.schemaname)
				free(f[i].handler.schemaname);
			if (f[i].handler.objectname)
				free(f[i].handler.objectname);
			if (f[i].validator.schemaname)
				free(f[i].validator.schemaname);
			if (f[i].validator.objectname)
				free(f[i].validator.objectname);
			free(f[i].owner);
			if (f[i].options)
				free(f[i].options);
			if (f[i].acl)
				free(f[i].acl);
			if (f[i].comment)
				free(f[i].comment);
		}

		free(f);
	}
}

void
dumpDropForeignDataWrapper(FILE *output, PQLForeignDataWrapper *f)
{
	char	*fdwname = formatObjectIdentifier(f->fdwname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP FOREIGN DATA WRAPPER %s;", fdwname);

	free(fdwname);
}

void
dumpCreateForeignDataWrapper(FILE *output, PQLForeignDataWrapper *f)
{
	char	*fdwname = formatObjectIdentifier(f->fdwname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE FOREIGN DATA WRAPPER %s", fdwname);

	if (f->handler.objectname)
		fprintf(output, " HANDLER %s.%s", f->handler.schemaname, f->handler.objectname);

	if (f->validator.objectname)
		fprintf(output, " VALIDATOR %s.%s", f->validator.schemaname, f->validator.objectname);

	if (f->options)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(f->options);
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

	/* comment */
	if (options.comment && f->comment != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "COMMENT ON FOREIGN DATA WRAPPER %s IS '%s';", fdwname, f->comment);
	}

	/* owner */
	if (options.owner)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s OWNER TO %s;", fdwname, f->owner);
	}

	/* privileges */
	/* XXX second tmp isn't used. Add an invalid PQLObject? */
	if (options.privileges)
	{
		PQLObject tmp;

		tmp.schemaname = NULL;
		tmp.objectname = f->fdwname;

		dumpGrantAndRevoke(output, PGQ_FOREIGN_DATA_WRAPPER, &tmp, &tmp, NULL, f->acl, NULL, NULL);
	}

	free(fdwname);
}

void
dumpAlterForeignDataWrapper(FILE *output, PQLForeignDataWrapper *a, PQLForeignDataWrapper *b)
{
	char	*fdwname1 = formatObjectIdentifier(a->fdwname);
	char	*fdwname2 = formatObjectIdentifier(b->fdwname);

	/* handler */
	if (a->handler.objectname == NULL && b->handler.objectname != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s HANDLER %s.%s;", fdwname2, b->handler.schemaname, b->handler.objectname);
	}
	else if (a->handler.objectname != NULL && b->handler.objectname == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s NO HANDLER;", fdwname1);
	}
	else if (a->handler.objectname != NULL && b->handler.objectname != NULL && compareRelations(&a->handler, &b->handler) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s HANDLER %s.%s;", fdwname2, b->handler.schemaname, b->handler.objectname);
	}

	/* validator */
	if (a->validator.objectname == NULL && b->validator.objectname != NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s VALIDATOR %s.%s;", fdwname2, b->validator.schemaname, b->validator.objectname);
	}
	else if (a->validator.objectname != NULL && b->validator.objectname == NULL)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s NO VALIDATOR;", fdwname1);
	}
	else if (a->validator.objectname != NULL && b->validator.objectname != NULL && compareRelations(&a->validator, &b->validator) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s VALIDATOR %s.%s;", fdwname2, b->validator.schemaname, b->validator.objectname);
	}

	/* options */
	if (a->options == NULL && b->options != NULL)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(b->options);
		fprintf(output, "\n\n");
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s OPTIONS (", fdwname2);
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
		fprintf(output, "ALTER FOREIGN DATA WRAPPER %s OPTIONS (", fdwname1);
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
			fprintf(output, "ALTER FOREIGN DATA WRAPPER %s OPTIONS (", fdwname2);
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
			fprintf(output, "ALTER FOREIGN DATA WRAPPER %s OPTIONS (", fdwname2);
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
			fprintf(output, "ALTER FOREIGN DATA WRAPPER %s OPTIONS (", fdwname2);
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

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON FOREIGN DATA WRAPPER %s IS '%s';", fdwname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON FOREIGN DATA WRAPPER %s IS NULL;", fdwname2);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			fprintf(output, "\n\n");
			fprintf(output, "ALTER FOREIGN DATA WRAPPER %s OWNER TO %s;", fdwname2, b->owner);
		}
	}

	/* privileges */
	if (options.privileges)
	{
		PQLObject tmpa, tmpb;

		tmpa.schemaname = NULL;
		tmpa.objectname = a->fdwname;
		tmpb.schemaname = NULL;
		tmpb.objectname = b->fdwname;

		if (a->acl != NULL || b->acl != NULL)
			dumpGrantAndRevoke(output, PGQ_FOREIGN_DATA_WRAPPER, &tmpa, &tmpb, a->acl, b->acl, NULL, NULL);
	}

	free(fdwname1);
	free(fdwname2);
}
