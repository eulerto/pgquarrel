/*
 * pgquarrel -- comparing database schemas
 *
 *  SUPPORTED
 * ~~~~~~~~~~~
 * comment: partial
 * domain: partial
 * event trigger: complete
 * extension: partial
 * function: partial
 * grant: partial
 * index: partial
 * language: partial
 * materialized view: partial
 * revoke: partial
 * rule: partial
 * schema: partial
 * sequence: partial
 * table: partial
 * trigger: partial
 * type: partial
 * view: partial
 *
 *  UNSUPPORTED
 * ~~~~~~~~~~~~~
 * foreign data wrapper
 * foreign table
 * server
 * user mapping
 * security label
 * text search { configuration | dictionary | parser | template }
 * aggregate
 * cast
 * collation
 * conversion
 *
 * operator
 * operator { class | family }
 *
 *  UNCERTAIN
 * ~~~~~~~~~~~~~
 *
 * ALTER DEFAULT PRIVILEGES
 * ALTER LARGE OBJECT
 *
 *
 * Copyright (c) 2015, Euler Taveira
 *
 */

#include "quarrel.h"
#include "common.h"

#include "domain.h"
#include "eventtrigger.h"
#include "extension.h"
#include "function.h"
#include "index.h"
#include "language.h"
#include "matview.h"
#include "rule.h"
#include "schema.h"
#include "sequence.h"
#include "table.h"
#include "trigger.h"
#include "type.h"
#include "view.h"

#include "mini-parser.h"


/* global variables */
enum PQLLogLevel	loglevel = PGQ_ERROR;
int					pgversion1;
int					pgversion2;
PGconn				*conn1;
PGconn				*conn2;

QuarrelOptions		options;

PQLStatistic		qstat = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0};

FILE				*fout;			/* output file */
FILE				*fpre, *fpost;	/* temporary files */

char				prepath[PGQMAXPATH];
char				postpath[PGQMAXPATH];


static int compareMajorVersion(int a, int b);
static bool parseBoolean(const char *key, const char *s);
static void help(void);
static void loadConfig(const char *c, QuarrelOptions *o);
static PGconn *connectDatabase(const char *host, const char *port,
							   const char *user, const char *password, const char *dbname);

static void mergeTempFiles(FILE *pre, FILE *post, FILE *output);
static FILE *openTempFile(char *p);
static void closeTempFile(FILE *fp, char *p);
static bool isEmptyFile(char *p);

static void quarrelDomains();
static void quarrelEventTriggers();
static void quarrelExtensions();
static void quarrelFunctions();
static void quarrelIndexes();
static void quarrelLanguages();
static void quarrelMaterializedViews();
static void quarrelRules();
static void quarrelSchemas();
static void quarrelSequences();
static void quarrelTables();
static void quarrelTriggers();
static void quarrelBaseTypes();
static void quarrelCompositeTypes();
static void quarrelEnumTypes();
static void quarrelRangeTypes();
static void quarrelTypes();
static void quarrelViews();

static int
compareMajorVersion(int a, int b)
{
	int c, d;

	c = (int) (a / 100);
	d = (int) (b / 100);

	if (c > d)
		return 1;
	else if (c < d)
		return -1;
	else
		return 0;
}

static bool
parseBoolean(const char *key, const char *s)
{
	/* false is not return if there is an error */
	bool	ret = false;	/* silence compiler */

	if (strcmp(s, "true") == 0 || strcmp(s, "1") == 0)
		ret = true;
	else if (strcmp(s, "false") == 0 || strcmp(s, "0") == 0)
		ret = false;
	else
		logError("invalid value for boolean option \"%s\": %s", key, s);

	return ret;
}

static void
help(void)
{
	printf("%s shows changes between database schemas.\n\n", PGQ_NAME);
	printf("Usage:\n");
	printf("  %s [OPTION]...\n", PGQ_NAME);
	printf("\nOptions:\n");
	printf("  -c, --config=FILENAME      configuration file\n");
	printf("  -s, --statistics           print statistics\n");
	printf("  -v, --verbose              verbose mode\n");
	printf("  --help                     show this help, then exit\n");
	printf("  --version                  output version information, then exit\n");
	printf("\nReport bugs to <euler@timbira.com.br>.\n");
}

static void
loadConfig(const char *cf, QuarrelOptions *options)
{
	MiniFile	*config;

	if (cf == NULL)
	{
		logError("config file is required");
		exit(EXIT_FAILURE);
	}

	config = mini_parse_file(cf);
	if (config != NULL)
	{
		Section		*sec;
		SectionData	*secdata;
		char		*tmp;


		logDebug("config file %s loaded", cf);

		for (sec = config->section; sec != NULL; sec = sec->next)
		{
			for (secdata = sec->data; secdata != NULL; secdata = secdata->next)
				logDebug("section: \"%s\" ; key: \"%s\" ; value: \"%s\"", sec->name,
						 secdata->key, secdata->value);
		}

		/* general options */
		tmp = mini_file_get_value(config, "general", "output");
		if (tmp != NULL)
			options->output = strdup(tmp);
		else
			options->output = strdup("quarrel.sql");		/* default */

		tmp = mini_file_get_value(config, "general", "tmpdir");
		if (tmp != NULL)
		{
			/* TODO improve some day... */
			if (strlen(tmp) > 256)
			{
				logError("tmpdir is too long (max: 256)");
				exit(EXIT_FAILURE);
			}
			options->tmpdir = strdup(tmp);
		}
		else
		{
			options->tmpdir = strdup("/tmp");				/* default */
		}

		if (mini_file_get_value(config, "general", "verbose") == NULL)
			options->verbose = false;	/* default */
		else
			options->verbose = parseBoolean("verbose", mini_file_get_value(config,
											"general", "verbose"));

		if (mini_file_get_value(config, "general", "statistics") == NULL)
			options->statistics = false;	/* default */
		else
			options->statistics = parseBoolean("statistics", mini_file_get_value(config,
											   "general", "statistics"));

		if (mini_file_get_value(config, "general", "comment") == NULL)
			options->comment = false;		/* default */
		else
			options->comment = parseBoolean("comment", mini_file_get_value(config,
											   "general", "comment"));

		if (mini_file_get_value(config, "general", "owner") == NULL)
			options->owner = false;		/* default */
		else
			options->owner = parseBoolean("owner", mini_file_get_value(config,
											   "general", "owner"));

		if (mini_file_get_value(config, "general", "privileges") == NULL)
			options->privileges = false;		/* default */
		else
			options->privileges = parseBoolean("privileges", mini_file_get_value(config,
											   "general", "privileges"));

		/* from options */
		tmp = mini_file_get_value(config, "from", "host");
		if (tmp != NULL)
			options->fhost = strdup(tmp);
		else
			options->fhost = NULL;

		tmp = mini_file_get_value(config, "from", "port");
		if (tmp != NULL)
			options->fport = strdup(tmp);
		else
			options->fport = NULL;

		tmp = mini_file_get_value(config, "from", "user");
		if (tmp != NULL)
			options->fusername = strdup(tmp);
		else
			options->fusername = NULL;

		tmp = mini_file_get_value(config, "from", "password");
		if (tmp != NULL)
			options->fpassword = strdup(tmp);
		else
			options->fpassword = NULL;

		tmp = mini_file_get_value(config, "from", "dbname");
		if (tmp != NULL)
			options->fdbname = strdup(tmp);
		else
			options->fdbname = NULL;


		/* to options */
		tmp = mini_file_get_value(config, "to", "host");
		if (tmp != NULL)
			options->thost = strdup(tmp);
		else
			options->thost = NULL;

		tmp = mini_file_get_value(config, "to", "port");
		if (tmp != NULL)
			options->tport = strdup(tmp);
		else
			options->tport = NULL;

		tmp = mini_file_get_value(config, "to", "user");
		if (tmp != NULL)
			options->tusername = strdup(tmp);
		else
			options->tusername = NULL;

		tmp = mini_file_get_value(config, "to", "password");
		if (tmp != NULL)
			options->tpassword = strdup(tmp);
		else
			options->tpassword = NULL;

		tmp = mini_file_get_value(config, "to", "dbname");
		if (tmp != NULL)
			options->tdbname = strdup(tmp);
		else
			options->tdbname = NULL;
	}
	else
	{
		logError("error while loading config file %s", cf);
		exit(EXIT_FAILURE);
	}

	/* free config file structure */
	mini_file_free(config);
}

static PGconn *
connectDatabase(const char *host, const char *port, const char *user,
				const char *password, const char *dbname)
{
	PGconn	*conn;

#define NUMBER_OF_PARAMS	7
	const char **keywords = malloc(NUMBER_OF_PARAMS * sizeof(*keywords));
	const char **values = malloc(NUMBER_OF_PARAMS * sizeof(*values));

	keywords[0] = "host";
	values[0] = host;
	keywords[1] = "port";
	values[1] = port;
	keywords[2] = "user";
	values[2] = user;
	keywords[3] = "password";
	values[3] = password;
	keywords[4] = "dbname";
	values[4] = dbname;
	keywords[5] = "fallback_application_name";
	values[5] = PGQ_NAME;
	keywords[6] = values[6] = NULL;

	conn = PQconnectdbParams(keywords, values, 1);

	free(keywords);
	free(values);

	if (conn == NULL)
	{
		logError("out of memory");
		exit(EXIT_FAILURE);
	}

	if (PQstatus(conn) == CONNECTION_BAD)
	{
		logError("connection to database failed: %s", PQerrorMessage(conn));
		PQfinish(conn);
		exit(EXIT_FAILURE);
	}

	return conn;
}

static void
quarrelDomains()
{
	PQLDomain	*domains1 = NULL;	/* from */
	PQLDomain	*domains2 = NULL;	/* to */
	int			ndomains1 = 0;		/* # of domains */
	int			ndomains2 = 0;
	int			i, j;

	/* Domains */
	domains1 = getDomains(conn1, &ndomains1);
	domains2 = getDomains(conn2, &ndomains2);

#ifdef PGQ_DEBUG
	for (i = 0; i < ndomains1; i++)
		logNoise("server1: %s.%s", domains1[i].obj.schemaname,
				 domains1[i].obj.objectname);

	for (i = 0; i < ndomains2; i++)
		logNoise("server2: %s.%s", domains2[i].obj.schemaname,
				 domains2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out domains not presented in the other list.
	 */
	i = j = 0;
	while (i < ndomains1 || j < ndomains2)
	{
		/* End of domains1 list. Print domains2 list until its end. */
		if (i == ndomains1)
		{
			logDebug("domain %s.%s: server2", domains2[j].obj.schemaname,
					 domains2[j].obj.objectname);

			getDomainConstraints(conn2, &domains2[j]);
			dumpCreateDomain(fpre, domains2[j]);

			j++;
			qstat.domainadded++;
		}
		/* End of domains2 list. Print domains1 list until its end. */
		else if (j == ndomains2)
		{
			logDebug("domain %s.%s: server1", domains1[i].obj.schemaname,
					 domains1[i].obj.objectname);

			dumpDropDomain(fpost, domains1[i]);

			i++;
			qstat.domainremoved++;
		}
		else if (compareRelations(domains1[i].obj, domains2[j].obj) == 0)
		{
			logDebug("domain %s.%s: server1 server2", domains1[i].obj.schemaname,
					 domains1[i].obj.objectname);

			getDomainConstraints(conn1, &domains1[i]);
			getDomainConstraints(conn2, &domains2[j]);
			dumpAlterDomain(fpre, domains1[i], domains2[j]);

			i++;
			j++;
		}
		else if (compareRelations(domains1[i].obj, domains2[j].obj) < 0)
		{
			logDebug("domain %s.%s: server1", domains1[i].obj.schemaname,
					 domains1[i].obj.objectname);

			dumpDropDomain(fpost, domains1[i]);

			i++;
			qstat.domainremoved++;
		}
		else if (compareRelations(domains1[i].obj, domains2[j].obj) > 0)
		{
			logDebug("domain %s.%s: server2", domains2[j].obj.schemaname,
					 domains2[j].obj.objectname);

			getDomainConstraints(conn2, &domains2[j]);
			dumpCreateDomain(fpre, domains2[j]);

			j++;
			qstat.domainadded++;
		}
	}
}

static void
quarrelEventTriggers()
{
	PQLEventTrigger		*evttrgs1 = NULL;		/* from */
	PQLEventTrigger		*evttrgs2 = NULL;		/* to */
	int			nevttrgs1 = 0;		/* # of evttrgs */
	int			nevttrgs2 = 0;
	int			i, j;

	evttrgs1 = getEventTriggers(conn1, &nevttrgs1);
	evttrgs2 = getEventTriggers(conn2, &nevttrgs2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nevttrgs1; i++)
		logNoise("server1: %s", evttrgs1[i].trgname);

	for (i = 0; i < nevttrgs2; i++)
		logNoise("server2: %s", evttrgs2[i].trgname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out evttrgs not presented in the other list.
	 */
	i = j = 0;
	while (i < nevttrgs1 || j < nevttrgs2)
	{
		/* End of evttrgs1 list. Print evttrgs2 list until its end. */
		if (i == nevttrgs1)
		{
			logDebug("event trigger %s: server2", evttrgs2[j].trgname);

			dumpCreateEventTrigger(fpre, evttrgs2[j]);

			j++;
			qstat.evttrgadded++;
		}
		/* End of evttrgs2 list. Print evttrgs1 list until its end. */
		else if (j == nevttrgs2)
		{
			logDebug("event trigger %s: server1", evttrgs1[i].trgname);

			dumpDropEventTrigger(fpost, evttrgs1[i]);

			i++;
			qstat.evttrgremoved++;
		}
		else if (strcmp(evttrgs1[i].trgname, evttrgs2[j].trgname) == 0)
		{
			logDebug("event trigger %s: server1 server2", evttrgs1[i].trgname);

			dumpAlterEventTrigger(fpre, evttrgs1[i], evttrgs2[j]);

			i++;
			j++;
		}
		else if (strcmp(evttrgs1[i].trgname, evttrgs2[j].trgname) < 0)
		{
			logDebug("event trigger %s: server1", evttrgs1[i].trgname);

			dumpDropEventTrigger(fpost, evttrgs1[i]);

			i++;
			qstat.evttrgremoved++;
		}
		else if (strcmp(evttrgs1[i].trgname, evttrgs2[j].trgname) > 0)
		{
			logDebug("event trigger %s: server2", evttrgs2[j].trgname);

			dumpCreateEventTrigger(fpre, evttrgs2[j]);

			j++;
			qstat.evttrgadded++;
		}
	}
}

static void
quarrelExtensions()
{
	PQLExtension		*extensions1 = NULL;		/* from */
	PQLExtension		*extensions2 = NULL;		/* to */
	int			nextensions1 = 0;		/* # of extensions */
	int			nextensions2 = 0;
	int			i, j;

	extensions1 = getExtensions(conn1, &nextensions1);
	extensions2 = getExtensions(conn2, &nextensions2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nextensions1; i++)
		logNoise("server1: %s", extensions1[i].extensionname);

	for (i = 0; i < nextensions2; i++)
		logNoise("server2: %s", extensions2[i].extensionname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out extensions not presented in the other list.
	 */
	i = j = 0;
	while (i < nextensions1 || j < nextensions2)
	{
		/* End of extensions1 list. Print extensions2 list until its end. */
		if (i == nextensions1)
		{
			logDebug("extension %s: server2", extensions2[j].extensionname);

			dumpCreateExtension(fpre, extensions2[j]);

			j++;
			qstat.extensionadded++;
		}
		/* End of extensions2 list. Print extensions1 list until its end. */
		else if (j == nextensions2)
		{
			logDebug("extension %s: server1", extensions1[i].extensionname);

			dumpDropExtension(fpost, extensions1[i]);

			i++;
			qstat.extensionremoved++;
		}
		else if (strcmp(extensions1[i].extensionname, extensions2[j].extensionname) == 0)
		{
			logDebug("extension %s: server1 server2", extensions1[i].extensionname);

			dumpAlterExtension(fpre, extensions1[i], extensions2[j]);

			i++;
			j++;
		}
		else if (strcmp(extensions1[i].extensionname, extensions2[j].extensionname) < 0)
		{
			logDebug("extension %s: server1", extensions1[i].extensionname);

			dumpDropExtension(fpost, extensions1[i]);

			i++;
			qstat.extensionremoved++;
		}
		else if (strcmp(extensions1[i].extensionname, extensions2[j].extensionname) > 0)
		{
			logDebug("extension %s: server2", extensions2[j].extensionname);

			dumpCreateExtension(fpre, extensions2[j]);

			j++;
			qstat.extensionadded++;
		}
	}
}

static void
quarrelFunctions()
{
	PQLFunction	*functions1 = NULL;	/* from */
	PQLFunction	*functions2 = NULL;	/* to */
	int			nfunctions1 = 0;		/* # of functions */
	int			nfunctions2 = 0;
	int			i, j;

	functions1 = getFunctions(conn1, &nfunctions1);
	functions2 = getFunctions(conn2, &nfunctions2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nfunctions1; i++)
		logNoise("server1: %s.%s(%s) %s", functions1[i].obj.schemaname,
				 functions1[i].obj.objectname, functions1[i].arguments,
				 functions1[i].returntype);

	for (i = 0; i < nfunctions2; i++)
		logNoise("server2: %s.%s(%s) %s", functions2[i].obj.schemaname,
				 functions2[i].obj.objectname, functions2[i].arguments,
				 functions2[i].returntype);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nfunctions1 || j < nfunctions2)
	{
		/* End of functions1 list. Print functions2 list until its end. */
		if (i == nfunctions1)
		{
			logDebug("function %s.%s(%s): server2", functions2[j].obj.schemaname,
					 functions2[j].obj.objectname, functions2[j].arguments);

			/*getFunctionAttributes(conn2, &functions2[j]);*/
			dumpCreateFunction(fpre, functions2[j], false);

			j++;
			qstat.functionadded++;
		}
		/* End of functions2 list. Print functions1 list until its end. */
		else if (j == nfunctions2)
		{
			logDebug("function %s.%s(%s): server1", functions1[i].obj.schemaname,
					 functions1[i].obj.objectname, functions1[i].arguments);

			dumpDropFunction(fpost, functions1[i]);

			i++;
			qstat.functionremoved++;
		}
		else if (compareFunctions(functions1[i], functions2[j]) == 0)
		{
			logDebug("function %s.%s(%s): server1 server2", functions1[i].obj.schemaname,
					 functions1[i].obj.objectname, functions1[i].arguments);

			/*getFunctionAttributes(conn1, &functions1[i]);
			getFunctionAttributes(conn2, &functions2[j]);*/
			/*
			 * When we change return type we have to recreate the function
			 * because there is no ALTER FUNCTION command for it.
			 */
			if (strcmp(functions1[i].returntype, functions2[j].returntype) == 0)
				dumpAlterFunction(fpre, functions1[i], functions2[j]);
			else
			{
				dumpDropFunction(fpre, functions1[i]);
				dumpCreateFunction(fpre, functions2[j], false);
			}

			i++;
			j++;
		}
		else if (compareFunctions(functions1[i], functions2[j]) < 0)
		{
			logDebug("function %s.%s(%s): server1", functions1[i].obj.schemaname,
					 functions1[i].obj.objectname, functions1[i].arguments);

			dumpDropFunction(fpost, functions1[i]);

			i++;
			qstat.functionremoved++;
		}
		else if (compareFunctions(functions1[i], functions2[j]) > 0)
		{
			logDebug("function %s.%s(%s): server2", functions2[j].obj.schemaname,
					 functions2[j].obj.objectname, functions2[j].arguments);

			/*getFunctionAttributes(conn2, &functions2[j]);*/
			dumpCreateFunction(fpre, functions2[j], false);

			j++;
			qstat.functionadded++;
		}
	}
}

static void
quarrelIndexes()
{
	PQLIndex	*indexes1 = NULL;	/* from */
	PQLIndex	*indexes2 = NULL;	/* to */
	int			nindexes1 = 0;		/* # of indexes */
	int			nindexes2 = 0;
	int			i, j;

	indexes1 = getIndexes(conn1, &nindexes1);
	indexes2 = getIndexes(conn2, &nindexes2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nindexes1; i++)
		logNoise("server1: %s.%s", indexes1[i].obj.schemaname,
				 indexes1[i].obj.objectname);

	for (i = 0; i < nindexes2; i++)
		logNoise("server2: %s.%s", indexes2[i].obj.schemaname,
				 indexes2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nindexes1 || j < nindexes2)
	{
		/* End of indexes1 list. Print indexes2 list until its end. */
		if (i == nindexes1)
		{
			logDebug("index %s.%s: server2", indexes2[j].obj.schemaname,
					 indexes2[j].obj.objectname);

			getIndexAttributes(conn2, &indexes2[j]);
			dumpCreateIndex(fpre, indexes2[j]);

			j++;
			qstat.indexadded++;
		}
		/* End of indexes2 list. Print indexes1 list until its end. */
		else if (j == nindexes2)
		{
			logDebug("index %s.%s: server1", indexes1[i].obj.schemaname,
					 indexes1[i].obj.objectname);

			dumpDropIndex(fpost, indexes1[i]);

			i++;
			qstat.indexremoved++;
		}
		else if (compareRelations(indexes1[i].obj, indexes2[j].obj) == 0)
		{
			logDebug("index %s.%s: server1 server2", indexes1[i].obj.schemaname,
					 indexes1[i].obj.objectname);

			getIndexAttributes(conn1, &indexes1[i]);
			getIndexAttributes(conn2, &indexes2[j]);
			dumpAlterIndex(fpre, indexes1[i], indexes2[j]);

			i++;
			j++;
		}
		else if (compareRelations(indexes1[i].obj, indexes2[j].obj) < 0)
		{
			logDebug("index %s.%s: server1", indexes1[i].obj.schemaname,
					 indexes1[i].obj.objectname);

			dumpDropIndex(fpost, indexes1[i]);

			i++;
			qstat.indexremoved++;
		}
		else if (compareRelations(indexes1[i].obj, indexes2[j].obj) > 0)
		{
			logDebug("index %s.%s: server2", indexes2[j].obj.schemaname,
					 indexes2[j].obj.objectname);

			getIndexAttributes(conn2, &indexes2[j]);
			dumpCreateIndex(fpre, indexes2[j]);

			j++;
			qstat.indexadded++;
		}
	}
}

static void
quarrelLanguages()
{
	PQLLanguage		*languages1 = NULL;		/* from */
	PQLLanguage		*languages2 = NULL;		/* to */
	int			nlanguages1 = 0;		/* # of languages */
	int			nlanguages2 = 0;
	int			i, j;

	languages1 = getLanguages(conn1, &nlanguages1);
	languages2 = getLanguages(conn2, &nlanguages2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nlanguages1; i++)
		logNoise("server1: %s", languages1[i].languagename);

	for (i = 0; i < nlanguages2; i++)
		logNoise("server2: %s", languages2[i].languagename);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out languages not presented in the other list.
	 */
	i = j = 0;
	while (i < nlanguages1 || j < nlanguages2)
	{
		/* End of languages1 list. Print languages2 list until its end. */
		if (i == nlanguages1)
		{
			logDebug("language %s: server2", languages2[j].languagename);

			dumpCreateLanguage(fpre, languages2[j]);

			j++;
			qstat.languageadded++;
		}
		/* End of languages2 list. Print languages1 list until its end. */
		else if (j == nlanguages2)
		{
			logDebug("language %s: server1", languages1[i].languagename);

			dumpDropLanguage(fpost, languages1[i]);

			i++;
			qstat.languageremoved++;
		}
		else if (strcmp(languages1[i].languagename, languages2[j].languagename) == 0)
		{
			logDebug("language %s: server1 server2", languages1[i].languagename);

			dumpAlterLanguage(fpre, languages1[i], languages2[j]);

			i++;
			j++;
		}
		else if (strcmp(languages1[i].languagename, languages2[j].languagename) < 0)
		{
			logDebug("language %s: server1", languages1[i].languagename);

			dumpDropLanguage(fpost, languages1[i]);

			i++;
			qstat.languageremoved++;
		}
		else if (strcmp(languages1[i].languagename, languages2[j].languagename) > 0)
		{
			logDebug("language %s: server2", languages2[j].languagename);

			dumpCreateLanguage(fpre, languages2[j]);

			j++;
			qstat.languageadded++;
		}
	}
}

static void
quarrelMaterializedViews()
{
	PQLMaterializedView	*matviews1 = NULL;	/* from */
	PQLMaterializedView	*matviews2 = NULL;	/* to */
	int			nmatviews1 = 0;		/* # of matviews */
	int			nmatviews2 = 0;
	int			i, j;

	matviews1 = getMaterializedViews(conn1, &nmatviews1);
	matviews2 = getMaterializedViews(conn2, &nmatviews2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nmatviews1; i++)
		logNoise("server1: %s.%s", matviews1[i].obj.schemaname,
				 matviews1[i].obj.objectname);

	for (i = 0; i < nmatviews2; i++)
		logNoise("server2: %s.%s", matviews2[i].obj.schemaname,
				 matviews2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nmatviews1 || j < nmatviews2)
	{
		/* End of matviews1 list. Print matviews2 list until its end. */
		if (i == nmatviews1)
		{
			logDebug("materialized view %s.%s: server2", matviews2[j].obj.schemaname,
					 matviews2[j].obj.objectname);

			getMaterializedViewAttributes(conn2, &matviews2[j]);
			dumpCreateMaterializedView(fpre, matviews2[j]);

			j++;
			qstat.matviewadded++;
		}
		/* End of matviews2 list. Print matviews1 list until its end. */
		else if (j == nmatviews2)
		{
			logDebug("materialized view %s.%s: server1", matviews1[i].obj.schemaname,
					 matviews1[i].obj.objectname);

			dumpDropMaterializedView(fpost, matviews1[i]);

			i++;
			qstat.matviewremoved++;
		}
		else if (compareRelations(matviews1[i].obj, matviews2[j].obj) == 0)
		{
			logDebug("materialized view %s.%s: server1 server2", matviews1[i].obj.schemaname,
					 matviews1[i].obj.objectname);

			getMaterializedViewAttributes(conn1, &matviews1[i]);
			getMaterializedViewAttributes(conn2, &matviews2[j]);
			dumpAlterMaterializedView(fpre, matviews1[i], matviews2[j]);

			i++;
			j++;
		}
		else if (compareRelations(matviews1[i].obj, matviews2[j].obj) < 0)
		{
			logDebug("materialized view %s.%s: server1", matviews1[i].obj.schemaname,
					 matviews1[i].obj.objectname);

			dumpDropMaterializedView(fpost, matviews1[i]);

			i++;
			qstat.matviewremoved++;
		}
		else if (compareRelations(matviews1[i].obj, matviews2[j].obj) > 0)
		{
			logDebug("materialized view %s.%s: server2", matviews2[j].obj.schemaname,
					 matviews2[j].obj.objectname);

			getMaterializedViewAttributes(conn2, &matviews2[j]);
			dumpCreateMaterializedView(fpre, matviews2[j]);

			j++;
			qstat.matviewadded++;
		}
	}
}

static void
quarrelRules()
{
	PQLRule	*rules1 = NULL;	/* from */
	PQLRule	*rules2 = NULL;	/* to */
	int			nrules1 = 0;		/* # of rules */
	int			nrules2 = 0;
	int			i, j;

	rules1 = getRules(conn1, &nrules1);
	rules2 = getRules(conn2, &nrules2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nrules1; i++)
		logNoise("server1: %s.%s", rules1[i].table.schemaname,
				 rules1[i].table.objectname);

	for (i = 0; i < nrules2; i++)
		logNoise("server2: %s.%s", rules2[i].table.schemaname,
				 rules2[i].table.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nrules1 || j < nrules2)
	{
		/* End of rules1 list. Print rules2 list until its end. */
		if (i == nrules1)
		{
			logDebug("rule %s.%s: server2", rules2[j].table.schemaname,
					 rules2[j].table.objectname);

			dumpCreateRule(fpre, rules2[j]);

			j++;
			qstat.ruleadded++;
		}
		/* End of rules2 list. Print rules1 list until its end. */
		else if (j == nrules2)
		{
			logDebug("rule %s.%s: server1", rules1[i].table.schemaname,
					 rules1[i].table.objectname);

			dumpDropRule(fpost, rules1[i]);

			i++;
			qstat.ruleremoved++;
		}
		else if (compareNamesAndRelations(rules1[i].table, rules2[j].table, rules1[i].rulename, rules2[j].rulename) == 0)
		{
			logDebug("rule %s.%s: server1 server2", rules1[i].table.schemaname,
					 rules1[i].table.objectname);

			dumpAlterRule(fpre, rules1[i], rules2[j]);

			i++;
			j++;
		}
		else if (compareNamesAndRelations(rules1[i].table, rules2[j].table, rules1[i].rulename, rules2[j].rulename) < 0)
		{
			logDebug("rule %s.%s: server1", rules1[i].table.schemaname,
					 rules1[i].table.objectname);

			dumpDropRule(fpost, rules1[i]);

			i++;
			qstat.ruleremoved++;
		}
		else if (compareNamesAndRelations(rules1[i].table, rules2[j].table, rules1[i].rulename, rules2[j].rulename) > 0)
		{
			logDebug("rule %s.%s: server2", rules2[j].table.schemaname,
					 rules2[j].table.objectname);

			dumpCreateRule(fpre, rules2[j]);

			j++;
			qstat.ruleadded++;
		}
	}
}

static void
quarrelSchemas()
{
	PQLSchema		*schemas1 = NULL;		/* from */
	PQLSchema		*schemas2 = NULL;		/* to */
	int			nschemas1 = 0;		/* # of schemas */
	int			nschemas2 = 0;
	int			i, j;

	schemas1 = getSchemas(conn1, &nschemas1);
	schemas2 = getSchemas(conn2, &nschemas2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nschemas1; i++)
		logNoise("server1: %s", schemas1[i].schemaname);

	for (i = 0; i < nschemas2; i++)
		logNoise("server2: %s", schemas2[i].schemaname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out schemas not presented in the other list.
	 */
	i = j = 0;
	while (i < nschemas1 || j < nschemas2)
	{
		/* End of schemas1 list. Print schemas2 list until its end. */
		if (i == nschemas1)
		{
			logDebug("schema %s: server2", schemas2[j].schemaname);

			dumpCreateSchema(fpre, schemas2[j]);

			j++;
			qstat.schemaadded++;
		}
		/* End of schemas2 list. Print schemas1 list until its end. */
		else if (j == nschemas2)
		{
			logDebug("schema %s: server1", schemas1[i].schemaname);

			dumpDropSchema(fpost, schemas1[i]);

			i++;
			qstat.schemaremoved++;
		}
		else if (strcmp(schemas1[i].schemaname, schemas2[j].schemaname) == 0)
		{
			logDebug("schema %s: server1 server2", schemas1[i].schemaname);

			dumpAlterSchema(fpre, schemas1[i], schemas2[j]);

			i++;
			j++;
		}
		else if (strcmp(schemas1[i].schemaname, schemas2[j].schemaname) < 0)
		{
			logDebug("schema %s: server1", schemas1[i].schemaname);

			dumpDropSchema(fpost, schemas1[i]);

			i++;
			qstat.schemaremoved++;
		}
		else if (strcmp(schemas1[i].schemaname, schemas2[j].schemaname) > 0)
		{
			logDebug("schema %s: server2", schemas2[j].schemaname);

			dumpCreateSchema(fpre, schemas2[j]);

			j++;
			qstat.schemaadded++;
		}
	}
}

static void
quarrelSequences()
{
	PQLSequence	*sequences1 = NULL;	/* from */
	PQLSequence	*sequences2 = NULL;	/* to */
	int			nsequences1 = 0;	/* # of sequences */
	int			nsequences2 = 0;
	int			i, j;

	sequences1 = getSequences(conn1, &nsequences1);
	sequences2 = getSequences(conn2, &nsequences2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nsequences1; i++)
		logNoise("server1: %s.%s", sequences1[i].obj.schemaname,
				 sequences1[i].obj.objectname);

	for (i = 0; i < nsequences2; i++)
		logNoise("server2: %s.%s", sequences2[i].obj.schemaname,
				 sequences2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nsequences1 || j < nsequences2)
	{
		/* End of sequences1 list. Print sequences2 list until its end. */
		if (i == nsequences1)
		{
			logDebug("sequence %s.%s: server2", sequences2[j].obj.schemaname,
					 sequences2[j].obj.objectname);

			getSequenceAttributes(conn2, &sequences2[j]);
			dumpCreateSequence(fpre, sequences2[j]);

			j++;
			qstat.seqadded++;
		}
		/* End of sequences2 list. Print sequences1 list until its end. */
		else if (j == nsequences2)
		{
			logDebug("sequence %s.%s: server1", sequences1[i].obj.schemaname,
					 sequences1[i].obj.objectname);

			dumpDropSequence(fpost, sequences1[i]);

			i++;
			qstat.seqremoved++;
		}
		else if (compareRelations(sequences1[i].obj, sequences2[j].obj) == 0)
		{
			logDebug("sequence %s.%s: server1 server2", sequences1[i].obj.schemaname,
					 sequences1[i].obj.objectname);

			getSequenceAttributes(conn1, &sequences1[i]);
			getSequenceAttributes(conn2, &sequences2[j]);
			dumpAlterSequence(fpre, sequences1[i], sequences2[j]);

			i++;
			j++;
		}
		else if (compareRelations(sequences1[i].obj, sequences2[j].obj) < 0)
		{
			logDebug("sequence %s.%s: server1", sequences1[i].obj.schemaname,
					 sequences1[i].obj.objectname);

			dumpDropSequence(fpost, sequences1[i]);

			i++;
			qstat.seqremoved++;
		}
		else if (compareRelations(sequences1[i].obj, sequences2[j].obj) > 0)
		{
			logDebug("sequence %s.%s: server2", sequences2[j].obj.schemaname,
					 sequences2[j].obj.objectname);

			getSequenceAttributes(conn2, &sequences2[j]);
			dumpCreateSequence(fpre, sequences2[j]);

			j++;
			qstat.seqadded++;
		}
	}
}

static void
quarrelTables()
{
	PQLTable	*tables1 = NULL;	/* from */
	PQLTable	*tables2 = NULL;	/* to */
	int			ntables1 = 0;		/* # of tables */
	int			ntables2 = 0;
	int			i, j;

	tables1 = getTables(conn1, &ntables1);
	getCheckConstraints(conn1, tables1, ntables1);
	getFKConstraints(conn1, tables1, ntables1);
	getPKConstraints(conn1, tables1, ntables1);

	tables2 = getTables(conn2, &ntables2);
	getCheckConstraints(conn2, tables2, ntables2);
	getFKConstraints(conn2, tables2, ntables2);
	getPKConstraints(conn2, tables2, ntables2);

	for (i = 0; i < ntables1; i++)
		logNoise("server1: %s.%s %u", tables1[i].obj.schemaname,
				 tables1[i].obj.objectname, tables1[i].obj.oid);

	for (i = 0; i < ntables2; i++)
		logNoise("server2: %s.%s %u", tables2[i].obj.schemaname,
				 tables2[i].obj.objectname, tables2[i].obj.oid);
#ifdef PGQ_DEBUG
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < ntables1 || j < ntables2)
	{
		/* End of tables1 list. Print tables2 list until its end. */
		if (i == ntables1)
		{
			logDebug("table %s.%s: server2", tables2[j].obj.schemaname,
					 tables2[j].obj.objectname);

			getTableAttributes(conn2, &tables2[j]);
			getOwnedBySequences(conn2, &tables2[j]);

			dumpCreateTable(fpre, tables2[j]);

			j++;
			qstat.tableadded++;
		}
		/* End of tables2 list. Print tables1 list until its end. */
		else if (j == ntables2)
		{
			logDebug("table %s.%s: server1", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			dumpDropTable(fpost, tables1[i]);

			i++;
			qstat.tableremoved++;
		}
		else if (compareRelations(tables1[i].obj, tables2[j].obj) == 0)
		{
			logDebug("table %s.%s: server1 server2", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			getTableAttributes(conn1, &tables1[i]);
			getTableAttributes(conn2, &tables2[j]);
			getOwnedBySequences(conn1, &tables1[i]);
			getOwnedBySequences(conn2, &tables2[j]);

			dumpAlterTable(fpre, tables1[i], tables2[j]);

			i++;
			j++;
		}
		else if (compareRelations(tables1[i].obj, tables2[j].obj) < 0)
		{
			logDebug("table %s.%s: server1", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			dumpDropTable(fpost, tables1[i]);

			i++;
			qstat.tableremoved++;
		}
		else if (compareRelations(tables1[i].obj, tables2[j].obj) > 0)
		{
			logDebug("table %s.%s: server2", tables2[j].obj.schemaname,
					 tables2[j].obj.objectname);

			getTableAttributes(conn2, &tables2[j]);
			getOwnedBySequences(conn2, &tables2[j]);

			dumpCreateTable(fpre, tables2[j]);

			j++;
			qstat.tableadded++;
		}
	}
}

static void
quarrelTriggers()
{
	PQLTrigger	*triggers1 = NULL;	/* from */
	PQLTrigger	*triggers2 = NULL;	/* to */
	int			ntriggers1 = 0;		/* # of triggers */
	int			ntriggers2 = 0;
	int			i, j;

	triggers1 = getTriggers(conn1, &ntriggers1);
	triggers2 = getTriggers(conn2, &ntriggers2);

#ifdef PGQ_DEBUG
	for (i = 0; i < ntriggers1; i++)
		logNoise("server1: %s.%s", triggers1[i].table.schemaname,
				 triggers1[i].table.objectname);

	for (i = 0; i < ntriggers2; i++)
		logNoise("server2: %s.%s", triggers2[i].table.schemaname,
				 triggers2[i].table.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < ntriggers1 || j < ntriggers2)
	{
		/* End of triggers1 list. Print triggers2 list until its end. */
		if (i == ntriggers1)
		{
			logDebug("trigger %s.%s: server2", triggers2[j].table.schemaname,
					 triggers2[j].table.objectname);

			dumpCreateTrigger(fpre, triggers2[j]);

			j++;
			qstat.trgadded++;
		}
		/* End of triggers2 list. Print triggers1 list until its end. */
		else if (j == ntriggers2)
		{
			logDebug("trigger %s.%s: server1", triggers1[i].table.schemaname,
					 triggers1[i].table.objectname);

			dumpDropTrigger(fpost, triggers1[i]);

			i++;
			qstat.trgremoved++;
		}
		else if (compareNamesAndRelations(triggers1[i].table, triggers2[j].table, triggers1[i].trgname, triggers2[j].trgname) == 0)
		{
			logDebug("trigger %s.%s: server1 server2", triggers1[i].table.schemaname,
					 triggers1[i].table.objectname);

			dumpAlterTrigger(fpre, triggers1[i], triggers2[j]);

			i++;
			j++;
		}
		else if (compareNamesAndRelations(triggers1[i].table, triggers2[j].table, triggers1[i].trgname, triggers2[j].trgname) < 0)
		{
			logDebug("trigger %s.%s: server1", triggers1[i].table.schemaname,
					 triggers1[i].table.objectname);

			dumpDropTrigger(fpost, triggers1[i]);

			i++;
			qstat.trgremoved++;
		}
		else if (compareNamesAndRelations(triggers1[i].table, triggers2[j].table, triggers1[i].trgname, triggers2[j].trgname) > 0)
		{
			logDebug("trigger %s.%s: server2", triggers2[j].table.schemaname,
					 triggers2[j].table.objectname);

			dumpCreateTrigger(fpre, triggers2[j]);

			j++;
			qstat.trgadded++;
		}
	}
}

static void
quarrelBaseTypes()
{
	PQLBaseType	*types1 = NULL;	/* from */
	PQLBaseType	*types2 = NULL;	/* to */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getBaseTypes(conn1, &ntypes1);
	types2 = getBaseTypes(conn2, &ntypes2);

#ifdef PGQ_DEBUG
	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < ntypes1 || j < ntypes2)
	{
		/* End of types1 list. Print types2 list until its end. */
		if (i == ntypes1)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateBaseType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropBaseType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpAlterBaseType(fpre, types1[i], types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropBaseType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateBaseType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
	}
}

static void
quarrelCompositeTypes()
{
	PQLCompositeType	*types1 = NULL;	/* from */
	PQLCompositeType	*types2 = NULL;	/* to */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getCompositeTypes(conn1, &ntypes1);
	types2 = getCompositeTypes(conn2, &ntypes2);

#ifdef PGQ_DEBUG
	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < ntypes1 || j < ntypes2)
	{
		/* End of types1 list. Print types2 list until its end. */
		if (i == ntypes1)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateCompositeType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropCompositeType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpAlterCompositeType(fpre, types1[i], types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropCompositeType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateCompositeType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
	}
}

static void
quarrelEnumTypes()
{
	PQLEnumType	*types1 = NULL;	/* from */
	PQLEnumType	*types2 = NULL;	/* to */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getEnumTypes(conn1, &ntypes1);
	types2 = getEnumTypes(conn2, &ntypes2);

#ifdef PGQ_DEBUG
	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < ntypes1 || j < ntypes2)
	{
		/* End of types1 list. Print types2 list until its end. */
		if (i == ntypes1)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateEnumType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropEnumType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpAlterEnumType(fpre, types1[i], types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropEnumType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateEnumType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
	}
}

static void
quarrelRangeTypes()
{
	PQLRangeType	*types1 = NULL;	/* from */
	PQLRangeType	*types2 = NULL;	/* to */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getRangeTypes(conn1, &ntypes1);
	types2 = getRangeTypes(conn2, &ntypes2);

#ifdef PGQ_DEBUG
	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < ntypes1 || j < ntypes2)
	{
		/* End of types1 list. Print types2 list until its end. */
		if (i == ntypes1)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateRangeType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropRangeType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpAlterRangeType(fpre, types1[i], types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropRangeType(fpost, types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(types1[i].obj, types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			dumpCreateRangeType(fpre, types2[j]);

			j++;
			qstat.typeadded++;
		}
	}
}

static void
quarrelTypes()
{
	quarrelBaseTypes();
	quarrelCompositeTypes();
	quarrelEnumTypes();
	quarrelRangeTypes();
}

static void
quarrelViews()
{
	PQLView	*views1 = NULL;	/* from */
	PQLView	*views2 = NULL;	/* to */
	int			nviews1 = 0;		/* # of views */
	int			nviews2 = 0;
	int			i, j;

	views1 = getViews(conn1, &nviews1);
	views2 = getViews(conn2, &nviews2);

#ifdef PGQ_DEBUG
	for (i = 0; i < nviews1; i++)
		logNoise("server1: %s.%s", views1[i].obj.schemaname,
				 views1[i].obj.objectname);

	for (i = 0; i < nviews2; i++)
		logNoise("server2: %s.%s", views2[i].obj.schemaname,
				 views2[i].obj.objectname);
#endif

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nviews1 || j < nviews2)
	{
		/* End of views1 list. Print views2 list until its end. */
		if (i == nviews1)
		{
			logDebug("view %s.%s: server2", views2[j].obj.schemaname,
					 views2[j].obj.objectname);

			dumpCreateView(fpre, views2[j]);

			j++;
			qstat.viewadded++;
		}
		/* End of views2 list. Print views1 list until its end. */
		else if (j == nviews2)
		{
			logDebug("view %s.%s: server1", views1[i].obj.schemaname,
					 views1[i].obj.objectname);

			dumpDropView(fpost, views1[i]);

			i++;
			qstat.viewremoved++;
		}
		else if (compareRelations(views1[i].obj, views2[j].obj) == 0)
		{
			logDebug("view %s.%s: server1 server2", views1[i].obj.schemaname,
					 views1[i].obj.objectname);

			dumpAlterView(fpre, views1[i], views2[j]);

			i++;
			j++;
		}
		else if (compareRelations(views1[i].obj, views2[j].obj) < 0)
		{
			logDebug("view %s.%s: server1", views1[i].obj.schemaname,
					 views1[i].obj.objectname);

			dumpDropView(fpost, views1[i]);

			i++;
			qstat.viewremoved++;
		}
		else if (compareRelations(views1[i].obj, views2[j].obj) > 0)
		{
			logDebug("view %s.%s: server2", views2[j].obj.schemaname,
					 views2[j].obj.objectname);

			dumpCreateView(fpre, views2[j]);

			j++;
			qstat.viewadded++;
		}
	}
}

static void
mergeTempFiles(FILE *pre, FILE *post, FILE *output)
{
	char	buf[4096];

	if (pre == NULL)
	{
		logError("file descriptor \"pre\" is invalid");
		exit(EXIT_FAILURE);
	}

	if (post == NULL)
	{
		logError("file descriptor \"post\" is invalid");
		exit(EXIT_FAILURE);
	}

	/* paranoia? */
	fflush(pre);
	fflush(post);

	/* start from the beginning */
	rewind(pre);
	rewind(post);

	while (fgets(buf, sizeof(buf), pre) != NULL)
	{
		if (fputs(buf, output) == EOF)
		{
			logError("could not write to temporary file \"%s\": %s", prepath, strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	/* EOF is expected to be reached. Check feof()? */
	if (ferror(pre) != 0)
	{
		logError("error while reading temporary file \"%s\": %s", prepath, strerror(errno));
		exit(EXIT_FAILURE);
	}

	while (fgets(buf, sizeof(buf), post) != NULL)
	{
		if (fputs(buf, output) == EOF)
		{
			logError("could not write to temporary file \"%s\": %s", postpath, strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	/* EOF is expected to be reached. Check feof()? */
	if (ferror(post) != 0)
	{
		logError("error while reading temporary file \"%s\": %s", postpath, strerror(errno));
		exit(EXIT_FAILURE);
	}
}

/*
 * Statements will be written temporarily in two files until the end of the
 * diff operation. At the end, files will be combined in the right order on the
 * output file and temporary files will be removed.
 */
static FILE *
openTempFile(char *p)
{
	FILE	*fp;

	fp = fopen(p, "w+");
	if (!fp)
	{
		logError("could not open temporary file \"%s\": %s", p, strerror(errno));
		exit(EXIT_FAILURE);
	}

	return fp;
}

/*
 * Close and remove the temporary file
 */
static void
closeTempFile(FILE *fp, char *p)
{
	fclose(fp);

	if (unlink(p))
	{
		logError("could not remove temporary file \"%s\": %s", p, strerror(errno));
		exit(EXIT_FAILURE);
	}
}

static bool
isEmptyFile(char *p)
{
	struct stat		buf;
	bool			r;

	if (stat(p, &buf) < 0)
	{
		logError("could not stat temporary file \"%s\": %s", p, strerror(errno));
		exit(EXIT_FAILURE);
	}

	logDebug("%s temporary file size: %d", p, buf.st_size);

	if (buf.st_size > 0)
		r = false;
	else
		r = true;

	return r;
}

static void
printStatistics(void)
{
	fprintf(stderr, "%d table(s) added, %d table(s) removed\n", qstat.tableadded,
			qstat.tableremoved);
	fprintf(stderr, "%d sequence(s) added, %d sequence(s) removed\n", qstat.seqadded,
			qstat.seqremoved);
	fprintf(stderr, "%d index(es) added, %d index(es) removed\n", qstat.indexadded,
			qstat.indexremoved);
}

int main(int argc, char *argv[])
{
	static struct option long_options[] =
	{
		{"config", required_argument, NULL, 'c'},
		{"ignore-version", no_argument, NULL, 'i'},
		{"statistics", no_argument, NULL, 's'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;

	char		*configfile = NULL;
	bool		statistics = false;
	int			ignoreversion = false;


	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0)
		{
			help();
			exit(EXIT_SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0)
		{
			printf(PGQ_NAME "" PGQ_VERSION "\n");
			exit(EXIT_SUCCESS);
		}
	}

	/* command-line options take precedence over config options */
	if (options.verbose)
		loglevel = PGQ_DEBUG;
	if (options.statistics)
		statistics = true;

	/* process command-line options */
	while ((c = getopt_long(argc, argv, "c:isv", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'c':
				configfile = strdup(optarg);
				break;
			case 'i':
				ignoreversion = true;
				break;
			case 's':
				statistics = true;
				break;
			case 'v':
				loglevel = PGQ_DEBUG;
				break;
			default:
				fprintf(stderr, "Try \"%s --help\" for more information.\n", PGQ_NAME);
				exit(EXIT_FAILURE);
		}
	}

	/* read configuration file */
	loadConfig(configfile, &options);

	/* connecting to server1 ... */
	conn1 = connectDatabase(options.fhost, options.fport, options.fusername,
							options.fpassword, options.fdbname);
	logDebug("connected to server1");

	/* is it a supported postgresql version? */
	pgversion1 = PQserverVersion(conn1);
	if (pgversion1 < PGQ_SUPPORTED)
	{
		const char *serverversion = PQparameterStatus(conn1, "server_version");
		logError("postgresql version %s is not supported (requires %s)",
				 serverversion ? serverversion : "'unknown'", PGQ_SUPPORTED_STR);
		PQfinish(conn1);
		exit(EXIT_FAILURE);
	}

	logDebug("server1 version: %s", PQparameterStatus(conn1, "server_version"));

	/* connecting to server2 ... */
	conn2 = connectDatabase(options.thost, options.tport, options.tusername,
							options.tpassword, options.tdbname);
	logDebug("connected to server2");

	/* is it a supported postgresql version? */
	pgversion2 = PQserverVersion(conn2);
	if (pgversion2 < PGQ_SUPPORTED)
	{
		const char *serverversion = PQparameterStatus(conn2, "server_version");
		logError("postgresql version %s is not supported (requires %s)",
				 serverversion ? serverversion : "'unknown'", PGQ_SUPPORTED_STR);
		PQfinish(conn2);
		PQfinish(conn1);
		exit(EXIT_FAILURE);
	}

	logDebug("server2 version: %s", PQparameterStatus(conn2, "server_version"));

	/*
	 * pgquarrel is using the reserved keywords provided by the postgres
	 * version that it was compiled in. If any of the involved servers has a
	 * version greater than the compiled one, some keywords could not be
	 * properly quoted. Resulting in a broken diff output. It happens with an
	 * old pgquarrel version or a to-be-released postgres version.  If you know
	 * what you are doing, use --ignore-version option.
	 */
	if (!ignoreversion && ((compareMajorVersion(pgversion1, PG_VERSION_NUM) > 0) ||
						   (compareMajorVersion(pgversion2, PG_VERSION_NUM) > 0)))
	{
		logError("cannot connect to server whose version (%s) is greater than postgres version (%s) used to compile pgquarrel",
				 (pgversion1 > pgversion2) ? PQparameterStatus(conn1,
						 "server_version") : PQparameterStatus(conn2, "server_version"), PG_VERSION);
		PQfinish(conn2);
		PQfinish(conn1);
		exit(EXIT_FAILURE);
	}

	/*
	 * Reports the possibility to have unsupported syntax if you diff a newer
	 * and an older server. Let it execute but generate a big warning.
	 * TODO move this check to each object
	 */
	if (pgversion2 < pgversion1)
		logWarning("unsupported syntax could be dumped while comparing server (%d) with server (%d)",
				   pgversion1, pgversion2);

	/* open output file */
	fout = fopen(options.output, "w");
	if (fout == NULL)
	{
		logError("could not open output file \"%s\"", options.output);
		exit(EXIT_FAILURE);
	}

	/* temporary files are used to put commands in the right dependency order */
	snprintf(prepath, PGQMAXPATH, "%s/quarrel.%d.pre", options.tmpdir, getpid());
	snprintf(postpath, PGQMAXPATH, "%s/quarrel.%d.post", options.tmpdir, getpid());
	fpre = openTempFile(prepath);
	fpost = openTempFile(postpath);

	/*
	 * Let's start the party ...
	 */

	/*
	 * We cannot start sending everything to stdout here because we will have
	 * problems with the dependencies. Generally, CREATE commands will be output
	 * at the beginning (in a pre-defined order) and DROP commands at the end
	 * (in a reverse order -- to solve dependency problems).
	 */

	quarrelLanguages();
	quarrelSchemas();
	quarrelExtensions();

	quarrelDomains();
	quarrelTypes();
	quarrelSequences();
	quarrelTables();
	quarrelIndexes();
	quarrelFunctions();
	quarrelViews();
	quarrelMaterializedViews();
	quarrelTriggers();
	quarrelRules();
	quarrelEventTriggers();

	/*
	 * Print header iff there is at least one command. Check if one of the
	 * files is not empty.
	 */
	fflush(fpre);
	fflush(fpost);
	if (!isEmptyFile(prepath) || !isEmptyFile(postpath))
	{
		fprintf(fout, "--\n-- pgquarrel %s\n", PGQ_VERSION);
		fprintf(fout, "-- quarrel between %s and %s\n", PQparameterStatus(conn1,
				"server_version"), PQparameterStatus(conn2, "server_version"));
		fprintf(fout, "--");
	}

	/* dump the quarrel in the right order */
	mergeTempFiles(fpre, fpost, fout);


	/* close and remove temporary files */
	closeTempFile(fpre, prepath);
	closeTempFile(fpost, postpath);

	/* closing connections ... */
	PQfinish(conn1);
	PQfinish(conn2);

	logDebug("server1 connection is closed");
	logDebug("server2 connection is closed");

	if (statistics)
		printStatistics();

	/* flush and close the output file */
	fflush(fout);
	fclose(fout);

	return 0;
}
