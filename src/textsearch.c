/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * textsearch.c
 *     Generate TEXT SEARCH commands
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * CREATE TEXT SEARCH CONFIGURATION
 * CREATE TEXT SEARCH DICTIONARY
 * CREATE TEXT SEARCH PARSER
 * CREATE TEXT SEARCH TEMPLATE
 * DROP TEXT SEARCH CONFIGURATION
 * DROP TEXT SEARCH DICTIONARY
 * DROP TEXT SEARCH PARSER
 * DROP TEXT SEARCH TEMPLATE
 * ALTER TEXT SEARCH CONFIGURATION
 * ALTER TEXT SEARCH DICTIONARY
 * ALTER TEXT SEARCH PARSER
 * ALTER TEXT SEARCH TEMPLATE
 * COMMENT ON TEXT SEARCH CONFIGURATION
 * COMMENT ON TEXT SEARCH DICTIONARY
 * COMMENT ON TEXT SEARCH PARSER
 * COMMENT ON TEXT SEARCH TEMPLATE
 *
 * TODO
 *
 * ALTER TEXT SEARCH CONFIGURATION ... ADD MAPPING FOR
 * ALTER TEXT SEARCH CONFIGURATION ... ALTER MAPPING FOR
 * ALTER TEXT SEARCH CONFIGURATION ... ALTER MAPPING FOR ... REPLACE
 * ALTER TEXT SEARCH CONFIGURATION ... RENAME TO
 * ALTER TEXT SEARCH DICTIONARY ... RENAME TO
 * ALTER TEXT SEARCH PARSER ... RENAME TO
 * ALTER TEXT SEARCH TEMPLATE ... RENAME TO
 * ALTER TEXT SEARCH CONFIGURATION ... SET SCHEMA
 * ALTER TEXT SEARCH DICTIONARY ... SET SCHEMA
 * ALTER TEXT SEARCH PARSER ... SET SCHEMA
 * ALTER TEXT SEARCH TEMPLATE ... SET SCHEMA
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "textsearch.h"

PQLTextSearchConfig *
getTextSearchConfigs(PGconn *c, int *n)
{
	char					*query;
	PQLTextSearchConfig		*d;
	PGresult				*res;
	int						i;

	logNoise("text search configuration: server version: %d", PQserverVersion(c));

	query = psprintf("SELECT c.oid, n.nspname, c.cfgname, quote_ident(o.nspname) || '.' || quote_ident(p.prsname) AS prsname, quote_ident(q.nspname) || '.' || quote_ident(d.dictname) AS dictname, (SELECT string_agg(alias, ', ') FROM ts_token_type(p.oid) AS t) AS tokentype, obj_description(c.oid, 'pg_ts_config') AS description, pg_get_userbyid(c.cfgowner) AS cfgowner FROM pg_ts_config c INNER JOIN pg_namespace n ON (c.cfgnamespace = n.oid) INNER JOIN pg_ts_parser p ON (c.cfgparser = p.oid) INNER JOIN pg_namespace o ON (p.prsnamespace = o.oid) INNER JOIN pg_ts_config_map m ON (c.oid = m.mapcfg) INNER JOIN pg_ts_dict d ON (m.mapdict = d.oid) INNER JOIN pg_namespace q ON (d.dictnamespace = q.oid) WHERE c.oid >= %u ORDER BY n.nspname, c.cfgname", PGQ_FIRST_USER_OID);

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

	*n = PQntuples(res);
	if (*n > 0)
		d = (PQLTextSearchConfig *) malloc(*n * sizeof(PQLTextSearchConfig));
	else
		d = NULL;

	logDebug("number of text search configurations in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		d[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		d[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		d[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "cfgname")));
		d[i].prs = strdup(PQgetvalue(res, i, PQfnumber(res, "prsname")));
		d[i].dict = strdup(PQgetvalue(res, i, PQfnumber(res, "dictname")));
		if (PQgetisnull(res, i, PQfnumber(res, "tokentype")))
			d[i].tokentype = NULL;
		else
			d[i].tokentype = strdup(PQgetvalue(res, i, PQfnumber(res, "tokentype")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			d[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			d[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (d[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		d[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "cfgowner")));

		logDebug("text search configuration \"%s\".\"%s\"", d[i].obj.schemaname,
				 d[i].obj.objectname);
	}

	PQclear(res);

	return d;
}

PQLTextSearchDict *
getTextSearchDicts(PGconn *c, int *n)
{
	char					*query;
	PQLTextSearchDict		*d;
	PGresult				*res;
	int						i;

	logNoise("text search dictionary: server version: %d", PQserverVersion(c));

	query = psprintf("SELECT d.oid, n.nspname, d.dictname, d.dictinitoption, quote_ident(o.nspname) || '.' || t.tmplname AS tmplname, obj_description(d.oid, 'pg_ts_dict') AS description, pg_get_userbyid(d.dictowner) AS dictowner FROM pg_ts_dict d INNER JOIN pg_namespace n ON (d.dictnamespace = n.oid) INNER JOIN pg_ts_template t ON (d.dicttemplate = t.oid) INNER JOIN pg_namespace o ON (t.tmplnamespace = o.oid) WHERE d.oid >= %u ORDER BY n.nspname, d.dictname", PGQ_FIRST_USER_OID);

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

	*n = PQntuples(res);
	if (*n > 0)
		d = (PQLTextSearchDict *) malloc(*n * sizeof(PQLTextSearchDict));
	else
		d = NULL;

	logDebug("number of text search dictionaries in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		d[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		d[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		d[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "dictname")));
		d[i].tmpl = strdup(PQgetvalue(res, i, PQfnumber(res, "tmplname")));
		if (PQgetisnull(res, i, PQfnumber(res, "dictinitoption")))
			d[i].options = NULL;
		else
			d[i].options = strdup(PQgetvalue(res, i, PQfnumber(res, "dictinitoption")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			d[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			d[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (d[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		d[i].owner = strdup(PQgetvalue(res, i, PQfnumber(res, "dictowner")));

		logDebug("text search dictionary \"%s\".\"%s\"", d[i].obj.schemaname,
				 d[i].obj.objectname);
	}

	PQclear(res);

	return d;
}

PQLTextSearchParser *
getTextSearchParsers(PGconn *c, int *n)
{
	char					*query;
	PQLTextSearchParser		*p;
	PGresult				*res;
	int						i;

	logNoise("text search parser: server version: %d", PQserverVersion(c));

	query = psprintf("SELECT p.oid, n.nspname, p.prsname, quote_ident(o.nspname) || '.' || quote_ident(a.proname) AS startfunc, quote_ident(q.nspname) || '.' || quote_ident(b.proname) AS tokenfunc, quote_ident(r.nspname) || '.' || quote_ident(a.proname) AS endfunc, quote_ident(s.nspname) || '.' || quote_ident(d.proname) AS lextypefunc, quote_ident(t.nspname) || '.' || quote_ident(e.proname) AS headlinefunc, obj_description(p.oid, 'pg_ts_parser') AS description FROM pg_ts_parser p INNER JOIN pg_namespace n ON (p.prsnamespace = n.oid) INNER JOIN (pg_proc a INNER JOIN pg_namespace o ON (a.pronamespace = o.oid)) ON (p.prsstart = a.oid) INNER JOIN (pg_proc b INNER JOIN pg_namespace q ON (b.pronamespace = q.oid)) ON (p.prstoken = b.oid) INNER JOIN (pg_proc c INNER JOIN pg_namespace r ON (c.pronamespace = r.oid)) ON (p.prsend = c.oid) INNER JOIN (pg_proc d INNER JOIN pg_namespace s ON (d.pronamespace = s.oid)) ON (p.prslextype = d.oid) LEFT JOIN (pg_proc e INNER JOIN pg_namespace t ON (e.pronamespace = t.oid)) ON (p.prsheadline = e.oid) WHERE p.oid >= %u ORDER BY n.nspname, p.prsname", PGQ_FIRST_USER_OID);

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

	*n = PQntuples(res);
	if (*n > 0)
		p = (PQLTextSearchParser *) malloc(*n * sizeof(PQLTextSearchParser));
	else
		p = NULL;

	logDebug("number of text search parsers in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		p[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		p[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		p[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "prsname")));
		p[i].startfunc = strdup(PQgetvalue(res, i, PQfnumber(res, "prsstart")));
		p[i].tokenfunc = strdup(PQgetvalue(res, i, PQfnumber(res, "prstoken")));
		p[i].endfunc = strdup(PQgetvalue(res, i, PQfnumber(res, "prsend")));
		p[i].lextypesfunc = strdup(PQgetvalue(res, i, PQfnumber(res, "prslextype")));
		if (PQgetisnull(res, i, PQfnumber(res, "prsheadline")))
			p[i].headlinefunc = NULL;
		else
			p[i].headlinefunc = strdup(PQgetvalue(res, i, PQfnumber(res, "prsheadline")));

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

		logDebug("text search parser \"%s\".\"%s\"", p[i].obj.schemaname,
				 p[i].obj.objectname);
	}

	PQclear(res);

	return p;
}

PQLTextSearchTemplate *
getTextSearchTemplates(PGconn *c, int *n)
{
	char					*query;
	PQLTextSearchTemplate	*t;
	PGresult				*res;
	int						i;

	logNoise("text search template: server version: %d", PQserverVersion(c));

	query = psprintf("SELECT t.oid, n.nspname, t.tmplname, quote_ident(o.nspname) || '.' || quote_ident(a.proname) AS tmpllexize, quote_ident(p.nspname) || '.' || quote_ident(b.proname) AS tmplinit, obj_description(t.oid, 'pg_ts_template') AS description FROM pg_ts_template t INNER JOIN pg_namespace n ON (t.tmplnamespace = n.oid) INNER JOIN (pg_proc a INNER JOIN pg_namespace o ON (a.pronamespace = o.oid)) ON (t.tmpllexize = a.oid) LEFT JOIN (pg_proc b INNER JOIN pg_namespace p ON (b.pronamespace = p.oid)) ON (t.tmplinit = b.oid) WHERE t.oid >= %u ORDER BY n.nspname, t.tmplname", PGQ_FIRST_USER_OID);

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

	*n = PQntuples(res);
	if (*n > 0)
		t = (PQLTextSearchTemplate *) malloc(*n * sizeof(PQLTextSearchTemplate));
	else
		t = NULL;

	logDebug("number of text search templates in server: %d", *n);

	for (i = 0; i < *n; i++)
	{
		char	*withoutescape;

		t[i].obj.oid = strtoul(PQgetvalue(res, i, PQfnumber(res, "oid")), NULL, 10);
		t[i].obj.schemaname = strdup(PQgetvalue(res, i, PQfnumber(res, "nspname")));
		t[i].obj.objectname = strdup(PQgetvalue(res, i, PQfnumber(res, "tmplname")));
		t[i].lexizefunc = strdup(PQgetvalue(res, i, PQfnumber(res, "tmpllexize")));
		if (PQgetisnull(res, i, PQfnumber(res, "tmplinit")))
			t[i].initfunc = NULL;
		else
			t[i].initfunc = strdup(PQgetvalue(res, i, PQfnumber(res, "tmplinit")));

		if (PQgetisnull(res, i, PQfnumber(res, "description")))
			t[i].comment = NULL;
		else
		{
			withoutescape = PQgetvalue(res, i, PQfnumber(res, "description"));
			t[i].comment = PQescapeLiteral(c, withoutescape, strlen(withoutescape));
			if (t[i].comment == NULL)
			{
				logError("escaping comment failed: %s", PQerrorMessage(c));
				PQclear(res);
				PQfinish(c);
				/* XXX leak another connection? */
				exit(EXIT_FAILURE);
			}
		}

		logDebug("text search template \"%s\".\"%s\"", t[i].obj.schemaname,
				 t[i].obj.objectname);
	}

	PQclear(res);

	return t;
}

void
freeTextSearchConfigs(PQLTextSearchConfig *c, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(c[i].obj.schemaname);
			free(c[i].obj.objectname);
			free(c[i].prs);
			free(c[i].dict);
			if (c[i].tokentype)
				free(c[i].tokentype);
			if (c[i].comment)
				PQfreemem(c[i].comment);
			free(c[i].owner);
		}

		free(c);
	}
}

void
freeTextSearchDicts(PQLTextSearchDict *d, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(d[i].obj.schemaname);
			free(d[i].obj.objectname);
			free(d[i].tmpl);
			if (d[i].options)
				free(d[i].options);
			if (d[i].comment)
				PQfreemem(d[i].comment);
			free(d[i].owner);
		}

		free(d);
	}
}

void
freeTextSearchParsers(PQLTextSearchParser *p, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(p[i].obj.schemaname);
			free(p[i].obj.objectname);
			free(p[i].startfunc);
			free(p[i].tokenfunc);
			free(p[i].endfunc);
			free(p[i].lextypesfunc);
			if (p[i].headlinefunc)
				free(p[i].headlinefunc);
			if (p[i].comment)
				PQfreemem(p[i].comment);
		}

		free(p);
	}
}

void
freeTextSearchTemplates(PQLTextSearchTemplate *t, int n)
{
	if (n > 0)
	{
		int	i;

		for (i = 0; i < n; i++)
		{
			free(t[i].obj.schemaname);
			free(t[i].obj.objectname);
			free(t[i].lexizefunc);
			if (t[i].initfunc)
				free(t[i].initfunc);
			if (t[i].comment)
				PQfreemem(t[i].comment);
		}

		free(t);
	}
}


void
dumpDropTextSearchConfig(FILE *output, PQLTextSearchConfig *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*cfgname = formatObjectIdentifier(c->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TEXT SEARCH CONFIGURATION %s.%s;",
			schema, cfgname);

	free(schema);
	free(cfgname);
}

void
dumpDropTextSearchDict(FILE *output, PQLTextSearchDict *d)
{
	char	*schema = formatObjectIdentifier(d->obj.schemaname);
	char	*dictname = formatObjectIdentifier(d->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TEXT SEARCH DICTIONARY %s.%s;",
			schema, dictname);

	free(schema);
	free(dictname);
}

void
dumpDropTextSearchParser(FILE *output, PQLTextSearchParser *p)
{
	char	*schema = formatObjectIdentifier(p->obj.schemaname);
	char	*prsname = formatObjectIdentifier(p->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TEXT SEARCH PARSER %s.%s;",
			schema, prsname);

	free(schema);
	free(prsname);
}

void
dumpDropTextSearchTemplate(FILE *output, PQLTextSearchTemplate *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tmplname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "DROP TEXT SEARCH TEMPLATE %s.%s;",
			schema, tmplname);

	free(schema);
	free(tmplname);
}


void
dumpCreateTextSearchConfig(FILE *output, PQLTextSearchConfig *c)
{
	char	*schema = formatObjectIdentifier(c->obj.schemaname);
	char	*cfgname = formatObjectIdentifier(c->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TEXT SEARCH CONFIGURATION %s.%s (", schema, cfgname);
	fprintf(output, "\nPARSER = %s", c->prs);
	fprintf(output, ");");

	free(schema);
	free(cfgname);
}

void
dumpCreateTextSearchDict(FILE *output, PQLTextSearchDict *d)
{
	char	*schema = formatObjectIdentifier(d->obj.schemaname);
	char	*dictname = formatObjectIdentifier(d->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TEXT SEARCH DICTIONARY %s.%s (", schema, dictname);
	fprintf(output, "\nTEMPLATE = %s", d->tmpl);
	if (d->options)
		fprintf(output, ",\n%s", d->options);
	fprintf(output, ");");

	free(schema);
	free(dictname);
}

void
dumpCreateTextSearchParser(FILE *output, PQLTextSearchParser *p)
{
	char	*schema = formatObjectIdentifier(p->obj.schemaname);
	char	*prsname = formatObjectIdentifier(p->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TEXT SEARCH PARSER %s.%s (", schema, prsname);
	fprintf(output, "\nSTART = %s", p->startfunc);
	fprintf(output, ",\nGETTOKEN = %s", p->tokenfunc);
	fprintf(output, ",\nEND = %s", p->endfunc);
	fprintf(output, ",\nLEXTYPES = %s", p->lextypesfunc);
	if (p->headlinefunc)
		fprintf(output, ",\nHEADLINE = %s", p->headlinefunc);
	fprintf(output, ");");

	free(schema);
	free(prsname);
}

void
dumpCreateTextSearchTemplate(FILE *output, PQLTextSearchTemplate *t)
{
	char	*schema = formatObjectIdentifier(t->obj.schemaname);
	char	*tmplname = formatObjectIdentifier(t->obj.objectname);

	fprintf(output, "\n\n");
	fprintf(output, "CREATE TEXT SEARCH TEMPLATE %s.%s (", schema, tmplname);
	fprintf(output, "\nLEXIZE = %s", t->lexizefunc);
	if (t->initfunc)
		fprintf(output, ",\nINIT = %s", t->initfunc);
	fprintf(output, ");");

	free(schema);
	free(tmplname);
}


void
dumpAlterTextSearchConfig(FILE *output, PQLTextSearchConfig *a,
						  PQLTextSearchConfig *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*cfgname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*cfgname2 = formatObjectIdentifier(b->obj.objectname);

	/* FIXME token_type support */

	/* dictionary */
	if (strcmp(a->dict, b->dict) != 0)
	{
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TEXT SEARCH CONFIGURATION %s.%s", schema2, cfgname2);
		fprintf(output, " ALTER MAPPING REPLACE %s WITH %s;", a->dict, b->dict);
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH CONFIGURATION %s.%s IS %s;",
					schema2, cfgname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH CONFIGURATION %s.%s IS NULL;",
					schema2, cfgname2);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER TEXT SEARCH CONFIGURATION %s.%s OWNER TO %s;",
					schema2,
					cfgname2,
					owner);

			free(owner);
		}
	}

	free(schema1);
	free(cfgname1);
	free(schema2);
	free(cfgname2);
}

void dumpAlterTextSearchDict(FILE *output, PQLTextSearchDict *a,
							 PQLTextSearchDict *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*dictname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*dictname2 = formatObjectIdentifier(b->obj.objectname);

	/* options */
	if (a->options == NULL && b->options != NULL)
	{
		stringList		*sl;
		stringListCell	*cell;
		bool			first = true;

		sl = buildStringList(b->options);
		fprintf(output, "\n\n");
		fprintf(output, "ALTER TEXT SEARCH DICTIONARY %s.%s (", schema2, dictname2);
		for (cell = sl->head; cell; cell = cell->next)
		{
			char	*str;

			str = strchr(cell->value, '=');
			*str++ = '\0';

			if (first)
			{
				fprintf(output, "%s = '%s'", cell->value, str);
				first = false;
			}
			else
				fprintf(output, ", %s = '%s'", cell->value, str);
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
		fprintf(output, "ALTER TEXT SEARCH DICTIONARY %s.%s (", schema1, dictname1);
		for (cell = sl->head; cell; cell = cell->next)
		{
			char	*str;

			str = strchr(cell->value, '=');
			*str = '\0';

			if (first)
			{
				fprintf(output, "%s", cell->value);
				first = false;
			}
			else
				fprintf(output, ", %s", cell->value);
		}
		fprintf(output, ");");

		freeStringList(sl);
	}
	else if (a->options != NULL && b->options != NULL &&
			 strcmp(a->options, b->options) != 0)
	{
		stringList	*rlist, *ilist, *slist;
		bool		first = true;

		/* reset options that are only presented in the first set */
		rlist = setOperationOptions(a->options, b->options, PGQ_SETDIFFERENCE, false,
									true);
		if (rlist)
		{
			stringListCell	*cell;

			if (first)
			{
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TEXT SEARCH DICTIONARY %s.%s (", schema1, dictname1);
			}
			for (cell = rlist->head; cell; cell = cell->next)
			{
				if (first)
				{
					fprintf(output, "%s", cell->value);
					first = false;
				}
				else
					fprintf(output, ", %s", cell->value);
			}

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

			if (first)
			{
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TEXT SEARCH DICTIONARY %s.%s (", schema1, dictname1);
			}
			for (cell = ilist->head; cell; cell = cell->next)
			{
				char	*str;

				str = strchr(cell->value, '=');
				*str++ = '\0';

				if (first)
				{
					fprintf(output, "%s = '%s'", cell->value, str);
					first = false;
				}
				else
					fprintf(output, ", %s = '%s'", cell->value, str);
			}

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

			if (first)
			{
				fprintf(output, "\n\n");
				fprintf(output, "ALTER TEXT SEARCH DICTIONARY %s.%s (", schema1, dictname1);
			}
			for (cell = slist->head; cell; cell = cell->next)
			{
				char	*str;

				str = strchr(cell->value, '=');
				*str++ = '\0';

				if (first)
				{
					fprintf(output, "%s = '%s'", cell->value, str);
					first = false;
				}
				else
					fprintf(output, ", %s = '%s'", cell->value, str);
			}

			freeStringList(slist);
		}

		if (!first)
			fprintf(output, ");");
	}

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH DICTIONARY %s.%s IS %s;",
					schema2, dictname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH DICTIONARY %s.%s IS NULL;",
					schema2, dictname2);
		}
	}

	/* owner */
	if (options.owner)
	{
		if (strcmp(a->owner, b->owner) != 0)
		{
			char	*owner = formatObjectIdentifier(b->owner);

			fprintf(output, "\n\n");
			fprintf(output, "ALTER TEXT SEARCH DICTIONARY %s.%s OWNER TO %s;",
					schema2,
					dictname2,
					owner);

			free(owner);
		}
	}

	free(schema1);
	free(dictname1);
	free(schema2);
	free(dictname2);
}

void dumpAlterTextSearchParser(FILE *output, PQLTextSearchParser *a,
							   PQLTextSearchParser *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*prsname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*prsname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH PARSER %s.%s IS %s;",
					schema2, prsname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH PARSER %s.%s IS NULL;",
					schema2, prsname2);
		}
	}

	free(schema1);
	free(prsname1);
	free(schema2);
	free(prsname2);
}

void dumpAlterTextSearchTemplate(FILE *output, PQLTextSearchTemplate *a,
								 PQLTextSearchTemplate *b)
{
	char	*schema1 = formatObjectIdentifier(a->obj.schemaname);
	char	*tmplname1 = formatObjectIdentifier(a->obj.objectname);
	char	*schema2 = formatObjectIdentifier(b->obj.schemaname);
	char	*tmplname2 = formatObjectIdentifier(b->obj.objectname);

	/* comment */
	if (options.comment)
	{
		if ((a->comment == NULL && b->comment != NULL) ||
				(a->comment != NULL && b->comment != NULL &&
				 strcmp(a->comment, b->comment) != 0))
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH TEMPLATE %s.%s IS %s;",
					schema2, tmplname2, b->comment);
		}
		else if (a->comment != NULL && b->comment == NULL)
		{
			fprintf(output, "\n\n");
			fprintf(output, "COMMENT ON TEXT SEARCH TEMPLATE %s.%s IS NULL;",
					schema2, tmplname2);
		}
	}

	free(schema1);
	free(tmplname1);
	free(schema2);
	free(tmplname2);
}
