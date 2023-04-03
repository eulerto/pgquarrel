/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 *  SUPPORTED
 * ~~~~~~~~~~~
 * access method: complete
 * aggregate: partial
 * cast: complete
 * collation: partial
 * comment: partial
 * conversion: partial
 * domain: partial
 * event trigger: complete
 * extension: partial
 * foreign data wrapper: complete
 * foreign table: partial
 * function: partial
 * grant: complete
 * index: partial
 * language: partial
 * materialized view: partial
 * operator: partial
 * operator class: partial
 * operator family: partial
 * policy: partial
 * procedure: partial
 * publication: partial
 * revoke: complete
 * rule: partial
 * schema: partial
 * security label: partial
 * sequence: partial
 * server: complete
 * subscription: partial
 * table: partial
 * transform: complete
 * trigger: partial
 * type: partial
 * text search configuration: partial
 * text search dictionary: partial
 * text search parser: partial
 * text search template: partial
 * user mapping: complete
 * view: partial
 *
 *  UNSUPPORTED
 * ~~~~~~~~~~~~~
 *
 *  UNCERTAIN
 * ~~~~~~~~~~~~~
 *
 * ALTER DEFAULT PRIVILEGES
 * ALTER LARGE OBJECT
 * GRANT LARGE OBJECT
 * REVOKE LARGE OBJECT
 *
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 *----------------------------------------------------------------------
 */

#include "quarrel.h"
#include "common.h"

#include "am.h"
#include "aggregate.h"
#include "cast.h"
#include "collation.h"
#include "conversion.h"
#include "domain.h"
#include "eventtrigger.h"
#include "extension.h"
#include "fdw.h"
#include "function.h"
#include "index.h"
#include "language.h"
#include "matview.h"
#include "operator.h"
#include "policy.h"
#include "publication.h"
#include "rule.h"
#include "schema.h"
#include "sequence.h"
#include "server.h"
#include "statistics.h"
#include "subscription.h"
#include "table.h"
#include "textsearch.h"
#include "transform.h"
#include "trigger.h"
#include "type.h"
#include "usermapping.h"
#include "view.h"

#include "mini-parser.h"


#define	MAX_PASSWORD_LEN	200

/* global variables */
enum PQLLogLevel	loglevel = PGQ_ERROR;
int					pgversion1;
int					pgversion2;
PGconn				*conn1;
PGconn				*conn2;

QuarrelGeneralOptions		options;	/* general options */

/* filter by schema */
char *include_schema_str;
char *exclude_schema_str;

PQLStatistic		qstat = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
							 0, 0
					  };

FILE				*fout;			/* output file */
FILE				*fpre, *fpost;	/* temporary files */

char				prepath[PGQMAXPATH];
char				postpath[PGQMAXPATH];


static int compareMajorVersion(int a, int b);
static bool parseBoolean(const char *key, const char *s);
static void help(void);
static void loadConfig(const char *c, QuarrelOptions *o);
static PGconn *connectDatabase(QuarrelDatabaseOptions opt);

static void mergeTempFiles(FILE *pre, FILE *post, FILE *output);
static FILE *openTempFile(char *p);
static void closeTempFile(FILE *fp, char *p);
static bool isEmptyFile(char *p);

static void quarrelAccessMethods();
static void quarrelAggregates();
static void quarrelCasts();
static void quarrelCollations();
static void quarrelConversions();
static void quarrelDomains();
static void quarrelEventTriggers();
static void quarrelExtensions();
static void quarrelForeignDataWrappers();
static void quarrelForeignServers();
static void quarrelForeignTables();
static void quarrelFunctions();
static void quarrelIndexes();
static void quarrelLanguages();
static void quarrelMaterializedViews();
static void quarrelOperators();
static void quarrelOperatorFamilies();
static void quarrelOperatorClasses();
static void quarrelPublications();
static void quarrelPolicies();
static void quarrelProcedures();
static void quarrelRules();
static void quarrelSchemas();
static void quarrelSequences();
static void quarrelStatistics();
static void quarrelSubscriptions();
static void quarrelTables();
static void quarrelTextSearchConfigs();
static void quarrelTextSearchDicts();
static void quarrelTextSearchParsers();
static void quarrelTextSearchTemplates();
static void quarrelTransforms();
static void quarrelTriggers();
static void quarrelBaseTypes();
static void quarrelCompositeTypes();
static void quarrelEnumTypes();
static void quarrelRangeTypes();
static void quarrelTypes();
static void quarrelUserMappings();
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
	QuarrelOptions	opts;

	loadConfig(NULL, &opts);

	printf("%s shows changes between database schemas.\n\n", PGQ_NAME);
	printf("Usage:\n");
	printf("  %s [OPTION]...\n", PGQ_NAME);
	printf("\nOptions:\n");
	printf("  -c, --config=FILENAME         configuration file\n");
	printf("  -f, --file=FILENAME           receive changes into this file, - for stdout (default: stdout)\n");
	printf("      --ignore-version          ignore version check\n");
	printf("  -s, --summary                 print a summary of changes\n");
	printf("  -t, --single-transaction      execute as a single transaction\n");
	printf("      --temp-directory=DIR      use as temporary file area (default: \"%s\")\n",
		   (opts.general.tmpdir) ? opts.general.tmpdir : "");
	printf("  -v, --verbose                 verbose mode\n");
	printf("\nObject options:\n");
	printf("      --access-method=BOOL      access method (default: %s)\n",
		   (opts.general.accessmethod) ? "true" : "false");
	printf("      --aggregate=BOOL          aggregate (default: %s)\n",
		   (opts.general.aggregate) ? "true" : "false");
	printf("      --cast=BOOL               cast (default: %s)\n",
		   (opts.general.cast) ? "true" : "false");
	printf("      --collation=BOOL          collation (default: %s)\n",
		   (opts.general.collation) ? "true" : "false");
	printf("      --comment=BOOL            comment (default: %s)\n",
		   (opts.general.comment) ? "true" : "false");
	printf("      --conversion=BOOL         conversion (default: %s)\n",
		   (opts.general.conversion) ? "true" : "false");
	printf("      --domain=BOOL             domain (default: %s)\n",
		   (opts.general.domain) ? "true" : "false");
	printf("      --event-trigger=BOOL      event trigger (default: %s)\n",
		   (opts.general.eventtrigger) ? "true" : "false");
	printf("      --extension=BOOL          extension (default: %s)\n",
		   (opts.general.extension) ? "true" : "false");
	printf("      --fdw=BOOL                foreign data wrapper (default: %s)\n",
		   (opts.general.fdw) ? "true" : "false");
	printf("      --foreign-table=BOOL      foreign table (default: %s)\n",
		   (opts.general.foreigntable) ? "true" : "false");
	printf("      --function=BOOL           function (default: %s)\n",
		   (opts.general.function) ? "true" : "false");
	printf("      --index=BOOL              index (default: %s)\n",
		   (opts.general.index) ? "true" : "false");
	printf("      --language=BOOL           language (default: %s)\n",
		   (opts.general.language) ? "true" : "false");
	printf("      --materialized-view=BOOL  materialized view (default: %s)\n",
		   (opts.general.matview) ? "true" : "false");
	printf("      --operator=BOOL           operator (default: %s)\n",
		   (opts.general.operator) ? "true" : "false");
	printf("      --owner=BOOL              owner (default: %s)\n",
		   (opts.general.owner) ? "true" : "false");
	printf("      --policy=BOOL             policy (default: %s)\n",
		   (opts.general.policy) ? "true" : "false");
	printf("      --publication=BOOL        publication (default: %s)\n",
		   (opts.general.publication) ? "true" : "false");
	printf("      --privileges=BOOL         privileges (default: %s)\n",
		   (opts.general.privileges) ? "true" : "false");
	printf("      --procedure=BOOL          procedure (default: %s)\n",
		   (opts.general.procedure) ? "true" : "false");
	printf("      --rule=BOOL               rule (default: %s)\n",
		   (opts.general.rule) ? "true" : "false");
	printf("      --schema=BOOL             schema (default: %s)\n",
		   (opts.general.schema) ? "true" : "false");
	printf("      --security-labels=BOOL    security labels (default: %s)\n",
		   (opts.general.securitylabels) ? "true" : "false");
	printf("      --sequence=BOOL           sequence (default: %s)\n",
		   (opts.general.sequence) ? "true" : "false");
	printf("      --statistics=BOOL         statistics (default: %s)\n",
		   (opts.general.statistics) ? "true" : "false");
	printf("      --subscription=BOOL       subscription (default: %s)\n",
		   (opts.general.subscription) ? "true" : "false");
	printf("      --table=BOOL              table (default: %s)\n",
		   (opts.general.table) ? "true" : "false");
	printf("      --table-partition=BOOL	table partition (default: %s)\n",
		   (opts.general.tablepartition) ? "true" : "false");		   
	printf("      --text-search=BOOL        text search (default: %s)\n",
		   (opts.general.textsearch) ? "true" : "false");
	printf("      --transform=BOOL          transform (default: %s)\n",
		   (opts.general.transform) ? "true" : "false");
	printf("      --trigger=BOOL            trigger (default: %s)\n",
		   (opts.general.trigger) ? "true" : "false");
	printf("      --type=BOOL               type (default: %s)\n",
		   (opts.general.type) ? "true" : "false");
	printf("      --view=BOOL               view (default: %s)\n",
		   (opts.general.view) ? "true" : "false");
	printf("\nFilter options:\n");
	printf("      --include-schema=PATTERN  include schemas that match PATTERN (default: all schemas)\n");
	printf("      --exclude-schema=PATTERN  exclude schemas that match PATTERN (default: none)\n");
	printf("\nSource options:\n");
	printf("      --source-dbname=DBNAME    database name or connection string\n");
	printf("      --source-host=HOSTNAME    server host or socket directory\n");
	printf("      --source-port=PORT        server port\n");
	printf("      --source-username=NAME    user name\n");
	printf("      --source-no-password      never prompt for password\n");
	printf("\nTarget options:\n");
	printf("      --target-dbname=DBNAME    database name or connection string\n");
	printf("      --target-host=HOSTNAME    server host or socket directory\n");
	printf("      --target-port=PORT        server port\n");
	printf("      --target-username=NAME    user name\n");
	printf("      --target-no-password      never prompt for password\n");
	printf("\n");
	printf("  --help                        show this help, then exit\n");
	printf("  --version                     output version information, then exit\n");
	printf("\nReport bugs to <euler@eulerto.com>.\n");
}

static void
loadConfig(const char *cf, QuarrelOptions *options)
{
	MiniFile	*config;

	/* default options */
	options->general.output = NULL;				/* general - output */
#if defined(_WINDOWS)
	options->general.tmpdir = strdup("c:/temp");	/* general - temp-directory */
#else
	options->general.tmpdir = strdup("/tmp");
#endif
	options->general.ignoreversion = false;		/* general - ignore-version */
	options->general.verbose = false;			/* general - verbose */
	options->general.summary = false;			/* general - summary */
	options->general.comment = false;			/* general - comment */
	options->general.securitylabels = false;	/* general - security-labels */
	options->general.owner = false;				/* general - owner */
	options->general.privileges = false;		/* general - privileges */
	options->general.singletxn = false;			/* general - single-transaction */

	options->general.accessmethod = false;		/* general - access method */
	options->general.aggregate = false;			/* general - aggregate */
	options->general.cast = false;				/* general - cast */
	options->general.collation = false;			/* general - collation */
	options->general.conversion = false;		/* general - conversion */
	options->general.domain = true;				/* general - domain */
	options->general.eventtrigger = false;		/* general - event-trigger */
	options->general.extension = false;			/* general - extension */
	options->general.fdw = false;				/* general - fdw */
	options->general.foreigntable = false;		/* general - foreign table */
	options->general.function = true;			/* general - function */
	options->general.index = true;				/* general - index */
	options->general.language = false;			/* general - language */
	options->general.matview = true;			/* general - materialized-view */
	options->general.operator = false;			/* general - operator */
	options->general.policy = false;			/* general - policy */
	options->general.procedure = true;			/* general - procedure */
	options->general.publication = false;		/* general - publication */
	options->general.rule = false;				/* general - rule */
	options->general.schema = true;				/* general - schema */
	options->general.sequence = true;			/* general - sequence */
	options->general.statistics = false;		/* general - statistics */
	options->general.subscription = false;		/* general - subscription */
	options->general.table = true;				/* general - table */
	options->general.tablepartition = true;		/* general - table-partition */
	options->general.textsearch = false;		/* general - text-search */
	options->general.transform = false;			/* general - transform */
	options->general.trigger = true;			/* general - trigger */
	options->general.type = true;				/* general - type */
	options->general.view = true;				/* general - view */

	options->general.include_schema = NULL;		/* general - include schemas that match pattern */
	options->general.exclude_schema = NULL;		/* general - exclude schemas that match pattern */

	options->source.host = NULL;				/* source - host */
	options->source.port = NULL;				/* source - port */
	options->source.username = NULL;			/* source - user */
	options->source.password = NULL;			/* source - password */
	options->source.dbname = NULL;				/* source - dbname */
	options->source.promptpassword = true;		/* source - prompt for password */

	options->target.host = NULL;				/* target - host */
	options->target.port = NULL;				/* target - port */
	options->target.username = NULL;			/* target - user */
	options->target.password = NULL;			/* target - password */
	options->target.dbname = NULL;				/* target - dbname */
	options->target.promptpassword = true;		/* source - prompt for password */

	/* if there is no config file, bail out */
	if (cf == NULL)
		return;

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
			options->general.output = strdup(tmp);

		tmp = mini_file_get_value(config, "general", "temp-directory");
		if (tmp != NULL)
		{
			/* TODO improve some day... */
			if (strlen(tmp) > 256)
			{
				logError("temp directory path is too long (max: 256)");
				exit(EXIT_FAILURE);
			}
			if (options->general.tmpdir)
				free(options->general.tmpdir);
			options->general.tmpdir = strdup(tmp);
		}
		else
		{
			/* TODO backward compatibility */
			tmp = mini_file_get_value(config, "general", "tmpdir");
			if (tmp != NULL)
			{
				/* TODO improve some day... */
				if (strlen(tmp) > 256)
				{
					logError("temp directory path is too long (max: 256)");
					exit(EXIT_FAILURE);
				}
				if (options->general.tmpdir)
					free(options->general.tmpdir);
				options->general.tmpdir = strdup(tmp);
			}
		}

		if (mini_file_get_value(config, "general", "verbose") != NULL)
			options->general.verbose = parseBoolean("verbose", mini_file_get_value(config,
													"general", "verbose"));

		if (mini_file_get_value(config, "general", "summary") != NULL)
			options->general.summary = parseBoolean("summary", mini_file_get_value(config,
													"general", "summary"));

		if (mini_file_get_value(config, "general", "comment") != NULL)
			options->general.comment = parseBoolean("comment", mini_file_get_value(config,
													"general", "comment"));

		if (mini_file_get_value(config, "general", "security-labels") != NULL)
			options->general.securitylabels = parseBoolean("security-labels",
											  mini_file_get_value(config,
													  "general", "security-labels"));

		if (mini_file_get_value(config, "general", "owner") != NULL)
			options->general.owner = parseBoolean("owner", mini_file_get_value(config,
												  "general", "owner"));

		if (mini_file_get_value(config, "general", "privileges") != NULL)
			options->general.privileges = parseBoolean("privileges",
										  mini_file_get_value(config,
												  "general", "privileges"));

		if (mini_file_get_value(config, "general", "ignore-version") != NULL)
			options->general.ignoreversion = parseBoolean("ignore-version",
											 mini_file_get_value(config,
													 "general", "ignore-version"));

		if (mini_file_get_value(config, "general", "single-transaction") != NULL)
			options->general.singletxn = parseBoolean("single-transaction",
										 mini_file_get_value(config,
												 "general", "single-transaction"));

		/*
		 * select objects that will be compared
		 */
		if (mini_file_get_value(config, "general", "access-method") != NULL)
			options->general.accessmethod = parseBoolean("access-method",
											mini_file_get_value(config,
													"general", "access-method"));

		if (mini_file_get_value(config, "general", "aggregate") != NULL)
			options->general.aggregate = parseBoolean("aggregate",
										 mini_file_get_value(config,
												 "general", "aggregate"));

		if (mini_file_get_value(config, "general", "cast") != NULL)
			options->general.cast = parseBoolean("cast", mini_file_get_value(config,
												 "general", "cast"));

		if (mini_file_get_value(config, "general", "collation") != NULL)
			options->general.collation = parseBoolean("collation",
										 mini_file_get_value(config,
												 "general", "collation"));

		if (mini_file_get_value(config, "general", "conversion") != NULL)
			options->general.conversion = parseBoolean("conversion",
										  mini_file_get_value(config,
												  "general", "conversion"));

		if (mini_file_get_value(config, "general", "domain") != NULL)
			options->general.domain = parseBoolean("domain", mini_file_get_value(config,
												   "general", "domain"));

		if (mini_file_get_value(config, "general", "event-trigger") != NULL)
			options->general.eventtrigger = parseBoolean("event-trigger",
											mini_file_get_value(config,
													"general", "event-trigger"));

		if (mini_file_get_value(config, "general", "extension") != NULL)
			options->general.extension = parseBoolean("extension",
										 mini_file_get_value(config,
												 "general", "extension"));

		if (mini_file_get_value(config, "general", "fdw") != NULL)
			options->general.fdw = parseBoolean("fdw", mini_file_get_value(config,
												"general", "fdw"));

		if (mini_file_get_value(config, "general", "foreign-table") != NULL)
			options->general.foreigntable = parseBoolean("foreign-table",
											mini_file_get_value(config,
													"general", "foreign-table"));

		if (mini_file_get_value(config, "general", "function") != NULL)
			options->general.function = parseBoolean("function", mini_file_get_value(config,
										"general", "function"));

		if (mini_file_get_value(config, "general", "index") != NULL)
			options->general.index = parseBoolean("index", mini_file_get_value(config,
												  "general", "index"));

		if (mini_file_get_value(config, "general", "language") != NULL)
			options->general.language = parseBoolean("language", mini_file_get_value(config,
										"general", "language"));

		if (mini_file_get_value(config, "general", "materialized-view") != NULL)
			options->general.matview = parseBoolean("materialized-view",
													mini_file_get_value(config,
															"general", "materialized-view"));

		if (mini_file_get_value(config, "general", "operator") != NULL)
			options->general.operator = parseBoolean("operator", mini_file_get_value(config,
										"general", "operator"));

		if (mini_file_get_value(config, "general", "policy") != NULL)
			options->general.policy = parseBoolean("policy", mini_file_get_value(config,
												   "general", "policy"));

		if (mini_file_get_value(config, "general", "procedure") != NULL)
			options->general.procedure = parseBoolean("procedure",
										 mini_file_get_value(config,
												 "general", "procedure"));

		if (mini_file_get_value(config, "general", "publication") != NULL)
			options->general.publication = parseBoolean("publication",
										   mini_file_get_value(config,
												   "general", "publication"));

		if (mini_file_get_value(config, "general", "rule") != NULL)
			options->general.rule = parseBoolean("rule", mini_file_get_value(config,
												 "general", "rule"));

		if (mini_file_get_value(config, "general", "schema") != NULL)
			options->general.schema = parseBoolean("schema", mini_file_get_value(config,
												   "general", "schema"));

		if (mini_file_get_value(config, "general", "sequence") != NULL)
			options->general.sequence = parseBoolean("sequence", mini_file_get_value(config,
										"general", "sequence"));

		if (mini_file_get_value(config, "general", "statistics") != NULL)
			options->general.statistics = parseBoolean("statistics",
										  mini_file_get_value(config,
												  "general", "statistics"));

		if (mini_file_get_value(config, "general", "subscription") != NULL)
			options->general.subscription = parseBoolean("subscription",
											mini_file_get_value(config,
													"general", "subscription"));

		if (mini_file_get_value(config, "general", "table") != NULL)
			options->general.table = parseBoolean("table", mini_file_get_value(config,
												  "general", "table"));

		if (mini_file_get_value(config, "general", "table-partition") != NULL)
			options->general.tablepartition = parseBoolean("table-partition", mini_file_get_value(config,
												  "general", "table-partition"));												  

		if (mini_file_get_value(config, "general", "text-search") != NULL)
			options->general.textsearch = parseBoolean("text-search",
										  mini_file_get_value(config,
												  "general", "text-search"));

		if (mini_file_get_value(config, "general", "transform") != NULL)
			options->general.transform = parseBoolean("transform",
										 mini_file_get_value(config,
												 "general", "transform"));

		if (mini_file_get_value(config, "general", "trigger") != NULL)
			options->general.trigger = parseBoolean("trigger", mini_file_get_value(config,
													"general", "trigger"));

		if (mini_file_get_value(config, "general", "type") != NULL)
			options->general.type = parseBoolean("type", mini_file_get_value(config,
												 "general", "type"));

		if (mini_file_get_value(config, "general", "view") != NULL)
			options->general.view = parseBoolean("view", mini_file_get_value(config,
												 "general", "view"));

		/* filter options */
		tmp = mini_file_get_value(config, "general", "include-schema");
		if (tmp != NULL)
			options->general.include_schema = strdup(tmp);

		tmp = mini_file_get_value(config, "general", "exclude-schema");
		if (tmp != NULL)
			options->general.exclude_schema = strdup(tmp);

		/* source options */
		tmp = mini_file_get_value(config, "source", "host");
		if (tmp != NULL)
			options->source.host = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "to", "host");
			if (tmp != NULL)
				options->source.host = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "source", "port");
		if (tmp != NULL)
			options->source.port = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "to", "port");
			if (tmp != NULL)
				options->source.port = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "source", "user");
		if (tmp != NULL)
			options->source.username = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "to", "user");
			if (tmp != NULL)
				options->source.username = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "source", "password");
		if (tmp != NULL)
			options->source.password = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "to", "password");
			if (tmp != NULL)
				options->source.password = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "source", "dbname");
		if (tmp != NULL)
			options->source.dbname = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "to", "dbname");
			if (tmp != NULL)
				options->source.dbname = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "source", "no-password");
		if (tmp != NULL)
			options->source.promptpassword = !parseBoolean("no-password",
											 mini_file_get_value(config, "source", "no-password"));


		/* target options */
		tmp = mini_file_get_value(config, "target", "host");
		if (tmp != NULL)
			options->target.host = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "from", "host");
			if (tmp != NULL)
				options->target.host = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "target", "port");
		if (tmp != NULL)
			options->target.port = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "from", "port");
			if (tmp != NULL)
				options->target.port = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "target", "user");
		if (tmp != NULL)
			options->target.username = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "from", "user");
			if (tmp != NULL)
				options->target.username = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "target", "password");
		if (tmp != NULL)
			options->target.password = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "from", "password");
			if (tmp != NULL)
				options->target.password = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "target", "dbname");
		if (tmp != NULL)
			options->target.dbname = strdup(tmp);
		else
		{
			tmp = mini_file_get_value(config, "from", "dbname");
			if (tmp != NULL)
				options->target.dbname = strdup(tmp);
		}

		tmp = mini_file_get_value(config, "target", "no-password");
		if (tmp != NULL)
			options->target.promptpassword = !parseBoolean("no-password",
											 mini_file_get_value(config, "target", "no-password"));
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
connectDatabase(QuarrelDatabaseOptions opt)
{
	PGconn		*conn;
	char		*prompt_password = NULL;

#define NUMBER_OF_PARAMS	7
	const char **keywords = malloc(NUMBER_OF_PARAMS * sizeof(*keywords));
	const char **values = malloc(NUMBER_OF_PARAMS * sizeof(*values));

	keywords[0] = "host";
	values[0] = opt.host;
	keywords[1] = "port";
	values[1] = opt.port;
	keywords[2] = "user";
	values[2] = opt.username;
	keywords[3] = "password";
	values[3] = opt.password;
	keywords[4] = "dbname";
	values[4] = opt.dbname;		/* database name or connection string */
	keywords[5] = "fallback_application_name";
	values[5] = PGQ_NAME;
	keywords[6] = values[6] = NULL;

	conn = PQconnectdbParams(keywords, values, 1);

	if (conn == NULL)
	{
		logError("out of memory");
		exit(EXIT_FAILURE);
	}

	/*
	 * Connection was not established. If the connection authentication method
	 * requires a password, asks one.
	 */
	if (PQstatus(conn) == CONNECTION_BAD && PQconnectionNeedsPassword(conn) &&
			opt.promptpassword)
	{
		PQfinish(conn);
#if PG_VERSION_NUM >= 140000
		if (opt.istarget)
			prompt_password = simple_prompt("Target password: ", false);
		else
			prompt_password = simple_prompt("Source password: ", false);
#elif PG_VERSION_NUM >= 100000
		prompt_password = (char *) malloc((MAX_PASSWORD_LEN + 1) * sizeof(char));
		if (opt.istarget)
			simple_prompt("Target password: ", prompt_password, MAX_PASSWORD_LEN + 1,
						  false);
		else
			simple_prompt("Source password: ", prompt_password, MAX_PASSWORD_LEN + 1,
						  false);
#else
		if (opt.istarget)
			prompt_password = simple_prompt("Target password: ", MAX_PASSWORD_LEN,
											false);
		else
			prompt_password = simple_prompt("Source password: ", MAX_PASSWORD_LEN,
											false);
#endif
		values[3] = prompt_password;

		/* try again with informed password */
		conn = PQconnectdbParams(keywords, values, 1);
	}

	free(keywords);
	free(values);

	if (prompt_password)
		free(prompt_password);

	/* failed connection attempt */
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		if (opt.istarget)
			logError("connection to target database failed: %s",
					 PQerrorMessage(conn));
		else
			logError("connection to source database failed: %s",
					 PQerrorMessage(conn));
		PQfinish(conn);
		exit(EXIT_FAILURE);
	}

	return conn;
}

static void
quarrelAccessMethods()
{
	PQLAccessMethod		*ams1 = NULL;		/* target */
	PQLAccessMethod		*ams2 = NULL;		/* source */
	int			nams1 = 0;		/* # of ams */
	int			nams2 = 0;
	int			i, j;

	ams1 = getAccessMethods(conn1, &nams1);
	ams2 = getAccessMethods(conn2, &nams2);

	for (i = 0; i < nams1; i++)
		logNoise("server1: %s", ams1[i].amname);

	for (i = 0; i < nams2; i++)
		logNoise("server2: %s", ams2[i].amname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out ams not presented in the other list.
	 */
	i = j = 0;
	while (i < nams1 || j < nams2)
	{
		/* End of ams1 list. Print ams2 list until its end. */
		if (i == nams1)
		{
			logDebug("am %s: server2", ams2[j].amname);

			dumpCreateAccessMethod(fpre, &ams2[j]);

			j++;
			qstat.amadded++;
		}
		/* End of ams2 list. Print ams1 list until its end. */
		else if (j == nams2)
		{
			logDebug("am %s: server1", ams1[i].amname);

			dumpDropAccessMethod(fpost, &ams1[i]);

			i++;
			qstat.amremoved++;
		}
		else if (strcmp(ams1[i].amname,
						ams2[j].amname) == 0)
		{
			logDebug("am %s: server1 server2", ams1[i].amname);

			dumpAlterAccessMethod(fpre, &ams1[i], &ams2[j]);

			i++;
			j++;
		}
		else if (strcmp(ams1[i].amname, ams2[j].amname) < 0)
		{
			logDebug("am %s: server1", ams1[i].amname);

			dumpDropAccessMethod(fpost, &ams1[i]);

			i++;
			qstat.amremoved++;
		}
		else if (strcmp(ams1[i].amname, ams2[j].amname) > 0)
		{
			logDebug("am %s: server2", ams2[j].amname);

			dumpCreateAccessMethod(fpre, &ams2[j]);

			j++;
			qstat.amadded++;
		}
	}

	freeAccessMethods(ams1, nams1);
	freeAccessMethods(ams2, nams2);
}

static void
quarrelAggregates()
{
	PQLAggregate	*aggregates1 = NULL;	/* target */
	PQLAggregate	*aggregates2 = NULL;	/* source */
	int				naggregates1 = 0;		/* # of aggregates */
	int				naggregates2 = 0;
	int				i, j;

	aggregates1 = getAggregates(conn1, &naggregates1);
	aggregates2 = getAggregates(conn2, &naggregates2);

	for (i = 0; i < naggregates1; i++)
		logNoise("server1: %s.%s(%s)", aggregates1[i].obj.schemaname,
				 aggregates1[i].obj.objectname, aggregates1[i].arguments);

	for (i = 0; i < naggregates2; i++)
		logNoise("server2: %s.%s(%s)", aggregates2[i].obj.schemaname,
				 aggregates2[i].obj.objectname, aggregates2[i].arguments);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < naggregates1 || j < naggregates2)
	{
		/* End of aggregates1 list. Print aggregates2 list until its end. */
		if (i == naggregates1)
		{
			logDebug("aggregate %s.%s(%s): server2", aggregates2[j].obj.schemaname,
					 aggregates2[j].obj.objectname, aggregates2[j].arguments);

			if (options.securitylabels)
				getAggregateSecurityLabels(conn2, &aggregates2[j]);

			dumpCreateAggregate(fpre, &aggregates2[j]);

			j++;
			qstat.aggadded++;
		}
		/* End of aggregates2 list. Print aggregates1 list until its end. */
		else if (j == naggregates2)
		{
			logDebug("aggregate %s.%s(%s): server1", aggregates1[i].obj.schemaname,
					 aggregates1[i].obj.objectname, aggregates1[i].arguments);

			dumpDropAggregate(fpost, &aggregates1[i]);

			i++;
			qstat.aggremoved++;
		}
		else if (compareAggregates(&aggregates1[i], &aggregates2[j]) == 0)
		{
			logDebug("aggregate %s.%s(%s): server1 server2", aggregates1[i].obj.schemaname,
					 aggregates1[i].obj.objectname, aggregates1[i].arguments);

			if (options.securitylabels)
			{
				getAggregateSecurityLabels(conn1, &aggregates1[i]);
				getAggregateSecurityLabels(conn2, &aggregates2[j]);
			}

			dumpAlterAggregate(fpre, &aggregates1[i], &aggregates2[j]);

			i++;
			j++;
		}
		else if (compareAggregates(&aggregates1[i], &aggregates2[j]) < 0)
		{
			logDebug("aggregate %s.%s(%s): server1", aggregates1[i].obj.schemaname,
					 aggregates1[i].obj.objectname, aggregates1[i].arguments);

			dumpDropAggregate(fpost, &aggregates1[i]);

			i++;
			qstat.aggremoved++;
		}
		else if (compareAggregates(&aggregates1[i], &aggregates2[j]) > 0)
		{
			logDebug("aggregate %s.%s(%s): server2", aggregates2[j].obj.schemaname,
					 aggregates2[j].obj.objectname, aggregates2[j].arguments);

			if (options.securitylabels)
				getAggregateSecurityLabels(conn2, &aggregates2[j]);

			dumpCreateAggregate(fpre, &aggregates2[j]);

			j++;
			qstat.aggadded++;
		}
	}

	freeAggregates(aggregates1, naggregates1);
	freeAggregates(aggregates2, naggregates2);
}

static void
quarrelCasts()
{
	PQLCast		*casts1 = NULL;		/* target */
	PQLCast		*casts2 = NULL;		/* source */
	int			ncasts1 = 0;		/* # of casts */
	int			ncasts2 = 0;
	int			i, j;

	/* Casts */
	casts1 = getCasts(conn1, &ncasts1);
	casts2 = getCasts(conn2, &ncasts2);

	for (i = 0; i < ncasts1; i++)
		logNoise("server1: cast %s AS %s", casts1[i].source, casts1[i].target);

	for (i = 0; i < ncasts2; i++)
		logNoise("server2: cast %s AS %s", casts2[i].source, casts2[i].target);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out casts not presented in the other list.
	 */
	i = j = 0;
	while (i < ncasts1 || j < ncasts2)
	{
		/* End of casts1 list. Print casts2 list until its end. */
		if (i == ncasts1)
		{
			logDebug("cast %s AS %s: server2", casts2[j].source, casts2[j].target);

			dumpCreateCast(fpre, &casts2[j]);

			j++;
			qstat.castadded++;
		}
		/* End of casts2 list. Print casts1 list until its end. */
		else if (j == ncasts2)
		{
			logDebug("cast %s AS %s: server1", casts1[i].source, casts1[i].target);

			dumpDropCast(fpost, &casts1[i]);

			i++;
			qstat.castremoved++;
		}
		else if (compareCasts(&casts1[i], &casts2[j]) == 0)
		{
			logDebug("cast %s AS %s: server1 server2", casts1[i].source, casts1[i].target);

			dumpAlterCast(fpre, &casts1[i], &casts2[j]);

			i++;
			j++;
		}
		else if (compareCasts(&casts1[i], &casts2[j]) < 0)
		{
			logDebug("cast %s AS %s: server1", casts1[i].source, casts1[i].target);

			dumpDropCast(fpost, &casts1[i]);

			i++;
			qstat.castremoved++;
		}
		else if (compareCasts(&casts1[i], &casts2[j]) > 0)
		{
			logDebug("cast %s AS %s: server2", casts2[j].source, casts2[j].target);

			dumpCreateCast(fpre, &casts2[j]);

			j++;
			qstat.castadded++;
		}
	}

	freeCasts(casts1, ncasts1);
	freeCasts(casts2, ncasts2);
}

static void
quarrelCollations()
{
	PQLCollation	*collations1 = NULL;	/* target */
	PQLCollation	*collations2 = NULL;	/* source */
	int			ncollations1 = 0;			/* # of collations */
	int			ncollations2 = 0;
	int			i, j;

	/* Collations */
	collations1 = getCollations(conn1, &ncollations1);
	collations2 = getCollations(conn2, &ncollations2);

	for (i = 0; i < ncollations1; i++)
		logNoise("server1: %s.%s", collations1[i].obj.schemaname,
				 collations1[i].obj.objectname);

	for (i = 0; i < ncollations2; i++)
		logNoise("server2: %s.%s", collations2[i].obj.schemaname,
				 collations2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out collations not presented in the other list.
	 */
	i = j = 0;
	while (i < ncollations1 || j < ncollations2)
	{
		/* End of collations1 list. Print collations2 list until its end. */
		if (i == ncollations1)
		{
			logDebug("collation %s.%s: server2", collations2[j].obj.schemaname,
					 collations2[j].obj.objectname);

			dumpCreateCollation(fpre, &collations2[j]);

			j++;
			qstat.collationadded++;
		}
		/* End of collations2 list. Print collations1 list until its end. */
		else if (j == ncollations2)
		{
			logDebug("collation %s.%s: server1", collations1[i].obj.schemaname,
					 collations1[i].obj.objectname);

			dumpDropCollation(fpost, &collations1[i]);

			i++;
			qstat.collationremoved++;
		}
		else if (compareRelations(&collations1[i].obj, &collations2[j].obj) == 0)
		{
			logDebug("collation %s.%s: server1 server2", collations1[i].obj.schemaname,
					 collations1[i].obj.objectname);

			dumpAlterCollation(fpre, &collations1[i], &collations2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&collations1[i].obj, &collations2[j].obj) < 0)
		{
			logDebug("collation %s.%s: server1", collations1[i].obj.schemaname,
					 collations1[i].obj.objectname);

			dumpDropCollation(fpost, &collations1[i]);

			i++;
			qstat.collationremoved++;
		}
		else if (compareRelations(&collations1[i].obj, &collations2[j].obj) > 0)
		{
			logDebug("collation %s.%s: server2", collations2[j].obj.schemaname,
					 collations2[j].obj.objectname);

			dumpCreateCollation(fpre, &collations2[j]);

			j++;
			qstat.collationadded++;
		}
	}

	freeCollations(collations1, ncollations1);
	freeCollations(collations2, ncollations2);
}

static void
quarrelConversions()
{
	PQLConversion	*conversions1 = NULL;	/* target */
	PQLConversion	*conversions2 = NULL;	/* source */
	int				nconversions1 = 0;		/* # of conversions */
	int				nconversions2 = 0;
	int				i, j;

	conversions1 = getConversions(conn1, &nconversions1);
	conversions2 = getConversions(conn2, &nconversions2);

	for (i = 0; i < nconversions1; i++)
		logNoise("server1: %s.%s %u", conversions1[i].obj.schemaname,
				 conversions1[i].obj.objectname, conversions1[i].obj.oid);

	for (i = 0; i < nconversions2; i++)
		logNoise("server2: %s.%s %u", conversions2[i].obj.schemaname,
				 conversions2[i].obj.objectname, conversions2[i].obj.oid);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nconversions1 || j < nconversions2)
	{
		/* End of conversions1 list. Print conversions2 list until its end. */
		if (i == nconversions1)
		{
			logDebug("conversion %s.%s: server2", conversions2[j].obj.schemaname,
					 conversions2[j].obj.objectname);

			dumpCreateConversion(fpre, &conversions2[j]);

			j++;
			qstat.conversionadded++;
		}
		/* End of conversions2 list. Print conversions1 list until its end. */
		else if (j == nconversions2)
		{
			logDebug("conversion %s.%s: server1", conversions1[i].obj.schemaname,
					 conversions1[i].obj.objectname);

			dumpDropConversion(fpost, &conversions1[i]);

			i++;
			qstat.conversionremoved++;
		}
		else if (compareRelations(&conversions1[i].obj, &conversions2[j].obj) == 0)
		{
			logDebug("conversion %s.%s: server1 server2", conversions1[i].obj.schemaname,
					 conversions1[i].obj.objectname);

			dumpAlterConversion(fpre, &conversions1[i], &conversions2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&conversions1[i].obj, &conversions2[j].obj) < 0)
		{
			logDebug("conversion %s.%s: server1", conversions1[i].obj.schemaname,
					 conversions1[i].obj.objectname);

			dumpDropConversion(fpost, &conversions1[i]);

			i++;
			qstat.conversionremoved++;
		}
		else if (compareRelations(&conversions1[i].obj, &conversions2[j].obj) > 0)
		{
			logDebug("conversion %s.%s: server2", conversions2[j].obj.schemaname,
					 conversions2[j].obj.objectname);

			dumpCreateConversion(fpre, &conversions2[j]);

			j++;
			qstat.conversionadded++;
		}
	}

	freeConversions(conversions1, nconversions1);
	freeConversions(conversions2, nconversions2);
}

static void
quarrelDomains()
{
	PQLDomain	*domains1 = NULL;	/* target */
	PQLDomain	*domains2 = NULL;	/* source */
	int			ndomains1 = 0;		/* # of domains */
	int			ndomains2 = 0;
	int			i, j;

	/* Domains */
	domains1 = getDomains(conn1, &ndomains1);
	domains2 = getDomains(conn2, &ndomains2);

	for (i = 0; i < ndomains1; i++)
		logNoise("server1: %s.%s", domains1[i].obj.schemaname,
				 domains1[i].obj.objectname);

	for (i = 0; i < ndomains2; i++)
		logNoise("server2: %s.%s", domains2[i].obj.schemaname,
				 domains2[i].obj.objectname);

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
			if (options.securitylabels)
				getDomainSecurityLabels(conn2, &domains2[j]);

			dumpCreateDomain(fpre, &domains2[j]);

			j++;
			qstat.domainadded++;
		}
		/* End of domains2 list. Print domains1 list until its end. */
		else if (j == ndomains2)
		{
			logDebug("domain %s.%s: server1", domains1[i].obj.schemaname,
					 domains1[i].obj.objectname);

			dumpDropDomain(fpost, &domains1[i]);

			i++;
			qstat.domainremoved++;
		}
		else if (compareRelations(&domains1[i].obj, &domains2[j].obj) == 0)
		{
			logDebug("domain %s.%s: server1 server2", domains1[i].obj.schemaname,
					 domains1[i].obj.objectname);

			getDomainConstraints(conn1, &domains1[i]);
			getDomainConstraints(conn2, &domains2[j]);
			if (options.securitylabels)
			{
				getDomainSecurityLabels(conn1, &domains1[i]);
				getDomainSecurityLabels(conn2, &domains2[j]);
			}

			dumpAlterDomain(fpre, &domains1[i], &domains2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&domains1[i].obj, &domains2[j].obj) < 0)
		{
			logDebug("domain %s.%s: server1", domains1[i].obj.schemaname,
					 domains1[i].obj.objectname);

			dumpDropDomain(fpost, &domains1[i]);

			i++;
			qstat.domainremoved++;
		}
		else if (compareRelations(&domains1[i].obj, &domains2[j].obj) > 0)
		{
			logDebug("domain %s.%s: server2", domains2[j].obj.schemaname,
					 domains2[j].obj.objectname);

			getDomainConstraints(conn2, &domains2[j]);
			if (options.securitylabels)
				getDomainSecurityLabels(conn2, &domains2[j]);

			dumpCreateDomain(fpre, &domains2[j]);

			j++;
			qstat.domainadded++;
		}
	}

	freeDomains(domains1, ndomains1);
	freeDomains(domains2, ndomains2);
}

static void
quarrelEventTriggers()
{
	PQLEventTrigger		*evttrgs1 = NULL;		/* target */
	PQLEventTrigger		*evttrgs2 = NULL;		/* source */
	int			nevttrgs1 = 0;		/* # of evttrgs */
	int			nevttrgs2 = 0;
	int			i, j;

	evttrgs1 = getEventTriggers(conn1, &nevttrgs1);
	evttrgs2 = getEventTriggers(conn2, &nevttrgs2);

	for (i = 0; i < nevttrgs1; i++)
		logNoise("server1: %s", evttrgs1[i].trgname);

	for (i = 0; i < nevttrgs2; i++)
		logNoise("server2: %s", evttrgs2[i].trgname);

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

			if (options.securitylabels)
				getEventTriggerSecurityLabels(conn2, &evttrgs2[j]);

			dumpCreateEventTrigger(fpre, &evttrgs2[j]);

			j++;
			qstat.evttrgadded++;
		}
		/* End of evttrgs2 list. Print evttrgs1 list until its end. */
		else if (j == nevttrgs2)
		{
			logDebug("event trigger %s: server1", evttrgs1[i].trgname);

			dumpDropEventTrigger(fpost, &evttrgs1[i]);

			i++;
			qstat.evttrgremoved++;
		}
		else if (strcmp(evttrgs1[i].trgname, evttrgs2[j].trgname) == 0)
		{
			logDebug("event trigger %s: server1 server2", evttrgs1[i].trgname);

			if (options.securitylabels)
			{
				getEventTriggerSecurityLabels(conn1, &evttrgs1[i]);
				getEventTriggerSecurityLabels(conn2, &evttrgs2[j]);
			}

			dumpAlterEventTrigger(fpre, &evttrgs1[i], &evttrgs2[j]);

			i++;
			j++;
		}
		else if (strcmp(evttrgs1[i].trgname, evttrgs2[j].trgname) < 0)
		{
			logDebug("event trigger %s: server1", evttrgs1[i].trgname);

			dumpDropEventTrigger(fpost, &evttrgs1[i]);

			i++;
			qstat.evttrgremoved++;
		}
		else if (strcmp(evttrgs1[i].trgname, evttrgs2[j].trgname) > 0)
		{
			logDebug("event trigger %s: server2", evttrgs2[j].trgname);

			if (options.securitylabels)
				getEventTriggerSecurityLabels(conn2, &evttrgs2[j]);

			dumpCreateEventTrigger(fpre, &evttrgs2[j]);

			j++;
			qstat.evttrgadded++;
		}
	}

	freeEventTriggers(evttrgs1, nevttrgs1);
	freeEventTriggers(evttrgs2, nevttrgs2);
}

static void
quarrelExtensions()
{
	PQLExtension		*extensions1 = NULL;		/* target */
	PQLExtension		*extensions2 = NULL;		/* source */
	int			nextensions1 = 0;		/* # of extensions */
	int			nextensions2 = 0;
	int			i, j;

	extensions1 = getExtensions(conn1, &nextensions1);
	extensions2 = getExtensions(conn2, &nextensions2);

	for (i = 0; i < nextensions1; i++)
		logNoise("server1: %s", extensions1[i].extensionname);

	for (i = 0; i < nextensions2; i++)
		logNoise("server2: %s", extensions2[i].extensionname);

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

			dumpCreateExtension(fpre, &extensions2[j]);

			j++;
			qstat.extensionadded++;
		}
		/* End of extensions2 list. Print extensions1 list until its end. */
		else if (j == nextensions2)
		{
			logDebug("extension %s: server1", extensions1[i].extensionname);

			dumpDropExtension(fpost, &extensions1[i]);

			i++;
			qstat.extensionremoved++;
		}
		else if (strcmp(extensions1[i].extensionname,
						extensions2[j].extensionname) == 0)
		{
			logDebug("extension %s: server1 server2", extensions1[i].extensionname);

			dumpAlterExtension(fpre, &extensions1[i], &extensions2[j]);

			i++;
			j++;
		}
		else if (strcmp(extensions1[i].extensionname, extensions2[j].extensionname) < 0)
		{
			logDebug("extension %s: server1", extensions1[i].extensionname);

			dumpDropExtension(fpost, &extensions1[i]);

			i++;
			qstat.extensionremoved++;
		}
		else if (strcmp(extensions1[i].extensionname, extensions2[j].extensionname) > 0)
		{
			logDebug("extension %s: server2", extensions2[j].extensionname);

			dumpCreateExtension(fpre, &extensions2[j]);

			j++;
			qstat.extensionadded++;
		}
	}

	freeExtensions(extensions1, nextensions1);
	freeExtensions(extensions2, nextensions2);
}

static void
quarrelForeignDataWrappers()
{
	PQLForeignDataWrapper		*fdws1 = NULL;		/* target */
	PQLForeignDataWrapper		*fdws2 = NULL;		/* source */
	int			nfdws1 = 0;		/* # of fdws */
	int			nfdws2 = 0;
	int			i, j;

	fdws1 = getForeignDataWrappers(conn1, &nfdws1);
	fdws2 = getForeignDataWrappers(conn2, &nfdws2);

	for (i = 0; i < nfdws1; i++)
		logNoise("server1: %s", fdws1[i].fdwname);

	for (i = 0; i < nfdws2; i++)
		logNoise("server2: %s", fdws2[i].fdwname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out fdws not presented in the other list.
	 */
	i = j = 0;
	while (i < nfdws1 || j < nfdws2)
	{
		/* End of fdws1 list. Print fdws2 list until its end. */
		if (i == nfdws1)
		{
			logDebug("fdw %s: server2", fdws2[j].fdwname);

			dumpCreateForeignDataWrapper(fpre, &fdws2[j]);

			j++;
			qstat.fdwadded++;
		}
		/* End of fdws2 list. Print fdws1 list until its end. */
		else if (j == nfdws2)
		{
			logDebug("fdw %s: server1", fdws1[i].fdwname);

			dumpDropForeignDataWrapper(fpost, &fdws1[i]);

			i++;
			qstat.fdwremoved++;
		}
		else if (strcmp(fdws1[i].fdwname,
						fdws2[j].fdwname) == 0)
		{
			logDebug("fdw %s: server1 server2", fdws1[i].fdwname);

			dumpAlterForeignDataWrapper(fpre, &fdws1[i], &fdws2[j]);

			i++;
			j++;
		}
		else if (strcmp(fdws1[i].fdwname, fdws2[j].fdwname) < 0)
		{
			logDebug("fdw %s: server1", fdws1[i].fdwname);

			dumpDropForeignDataWrapper(fpost, &fdws1[i]);

			i++;
			qstat.fdwremoved++;
		}
		else if (strcmp(fdws1[i].fdwname, fdws2[j].fdwname) > 0)
		{
			logDebug("fdw %s: server2", fdws2[j].fdwname);

			dumpCreateForeignDataWrapper(fpre, &fdws2[j]);

			j++;
			qstat.fdwadded++;
		}
	}

	freeForeignDataWrappers(fdws1, nfdws1);
	freeForeignDataWrappers(fdws2, nfdws2);
}

static void
quarrelForeignServers()
{
	PQLForeignServer	*servers1 = NULL;		/* target */
	PQLForeignServer	*servers2 = NULL;		/* source */
	int			nservers1 = 0;		/* # of servers */
	int			nservers2 = 0;
	int			i, j;

	servers1 = getForeignServers(conn1, &nservers1);
	servers2 = getForeignServers(conn2, &nservers2);

	for (i = 0; i < nservers1; i++)
		logNoise("server1: %s", servers1[i].servername);

	for (i = 0; i < nservers2; i++)
		logNoise("server2: %s", servers2[i].servername);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out servers not presented in the other list.
	 */
	i = j = 0;
	while (i < nservers1 || j < nservers2)
	{
		/* End of servers1 list. Print servers2 list until its end. */
		if (i == nservers1)
		{
			logDebug("server %s: server2", servers2[j].servername);

			dumpCreateForeignServer(fpre, &servers2[j]);

			j++;
			qstat.serveradded++;
		}
		/* End of servers2 list. Print servers1 list until its end. */
		else if (j == nservers2)
		{
			logDebug("server %s: server1", servers1[i].servername);

			dumpDropForeignServer(fpost, &servers1[i]);

			i++;
			qstat.serverremoved++;
		}
		else if (strcmp(servers1[i].servername,
						servers2[j].servername) == 0)
		{
			logDebug("server %s: server1 server2", servers1[i].servername);

			dumpAlterForeignServer(fpre, &servers1[i], &servers2[j]);

			i++;
			j++;
		}
		else if (strcmp(servers1[i].servername, servers2[j].servername) < 0)
		{
			logDebug("server %s: server1", servers1[i].servername);

			dumpDropForeignServer(fpost, &servers1[i]);

			i++;
			qstat.serverremoved++;
		}
		else if (strcmp(servers1[i].servername, servers2[j].servername) > 0)
		{
			logDebug("server %s: server2", servers2[j].servername);

			dumpCreateForeignServer(fpre, &servers2[j]);

			j++;
			qstat.serveradded++;
		}
	}

	freeForeignServers(servers1, nservers1);
	freeForeignServers(servers2, nservers2);
}

static void
quarrelFunctions()
{
	PQLFunction	*functions1 = NULL;	/* target */
	PQLFunction	*functions2 = NULL;	/* source */
	int			nfunctions1 = 0;		/* # of functions */
	int			nfunctions2 = 0;
	int			i, j;

	functions1 = getFunctions(conn1, &nfunctions1);
	functions2 = getFunctions(conn2, &nfunctions2);

	for (i = 0; i < nfunctions1; i++)
		logNoise("server1: %s.%s(%s) %s", functions1[i].obj.schemaname,
				 functions1[i].obj.objectname, functions1[i].arguments,
				 functions1[i].returntype);

	for (i = 0; i < nfunctions2; i++)
		logNoise("server2: %s.%s(%s) %s", functions2[i].obj.schemaname,
				 functions2[i].obj.objectname, functions2[i].arguments,
				 functions2[i].returntype);

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

			if (options.securitylabels)
				getFunctionSecurityLabels(conn2, &functions2[j]);

			dumpCreateFunction(fpre, &functions2[j], false);

			j++;
			qstat.functionadded++;
		}
		/* End of functions2 list. Print functions1 list until its end. */
		else if (j == nfunctions2)
		{
			logDebug("function %s.%s(%s): server1", functions1[i].obj.schemaname,
					 functions1[i].obj.objectname, functions1[i].arguments);

			dumpDropFunction(fpost, &functions1[i]);

			i++;
			qstat.functionremoved++;
		}
		else if (compareFunctions(&functions1[i], &functions2[j]) == 0)
		{
			logDebug("function %s.%s(%s): server1 server2", functions1[i].obj.schemaname,
					 functions1[i].obj.objectname, functions1[i].arguments);

			if (options.securitylabels)
			{
				getFunctionSecurityLabels(conn1, &functions1[i]);
				getFunctionSecurityLabels(conn2, &functions2[j]);
			}

			/*
			 * When we change return type we have to recreate the function
			 * because there is no ALTER FUNCTION command for it.
			 */
			if (strcmp(functions1[i].returntype, functions2[j].returntype) == 0)
				dumpAlterFunction(fpre, &functions1[i], &functions2[j]);
			else
			{
				dumpDropFunction(fpre, &functions1[i]);
				dumpCreateFunction(fpre, &functions2[j], false);
			}

			i++;
			j++;
		}
		else if (compareFunctions(&functions1[i], &functions2[j]) < 0)
		{
			logDebug("function %s.%s(%s): server1", functions1[i].obj.schemaname,
					 functions1[i].obj.objectname, functions1[i].arguments);

			dumpDropFunction(fpost, &functions1[i]);

			i++;
			qstat.functionremoved++;
		}
		else if (compareFunctions(&functions1[i], &functions2[j]) > 0)
		{
			logDebug("function %s.%s(%s): server2", functions2[j].obj.schemaname,
					 functions2[j].obj.objectname, functions2[j].arguments);

			if (options.securitylabels)
				getFunctionSecurityLabels(conn2, &functions2[j]);

			dumpCreateFunction(fpre, &functions2[j], false);

			j++;
			qstat.functionadded++;
		}
	}

	freeFunctions(functions1, nfunctions1);
	freeFunctions(functions2, nfunctions2);
}

static void
quarrelIndexes()
{
	PQLIndex	*indexes1 = NULL;	/* target */
	PQLIndex	*indexes2 = NULL;	/* source */
	int			nindexes1 = 0;		/* # of indexes */
	int			nindexes2 = 0;
	int			i, j;

	indexes1 = getIndexes(conn1, &nindexes1);
	indexes2 = getIndexes(conn2, &nindexes2);

	for (i = 0; i < nindexes1; i++)
		logNoise("server1: %s.%s", indexes1[i].obj.schemaname,
				 indexes1[i].obj.objectname);

	for (i = 0; i < nindexes2; i++)
		logNoise("server2: %s.%s", indexes2[i].obj.schemaname,
				 indexes2[i].obj.objectname);

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

			dumpCreateIndex(fpre, &indexes2[j]);

			j++;
			qstat.indexadded++;
		}
		/* End of indexes2 list. Print indexes1 list until its end. */
		else if (j == nindexes2)
		{
			logDebug("index %s.%s: server1", indexes1[i].obj.schemaname,
					 indexes1[i].obj.objectname);

			dumpDropIndex(fpost, &indexes1[i]);

			i++;
			qstat.indexremoved++;
		}
		else if (compareRelations(&indexes1[i].obj, &indexes2[j].obj) == 0)
		{
			logDebug("index %s.%s: server1 server2", indexes1[i].obj.schemaname,
					 indexes1[i].obj.objectname);

			dumpAlterIndex(fpre, &indexes1[i], &indexes2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&indexes1[i].obj, &indexes2[j].obj) < 0)
		{
			logDebug("index %s.%s: server1", indexes1[i].obj.schemaname,
					 indexes1[i].obj.objectname);

			dumpDropIndex(fpost, &indexes1[i]);

			i++;
			qstat.indexremoved++;
		}
		else if (compareRelations(&indexes1[i].obj, &indexes2[j].obj) > 0)
		{
			logDebug("index %s.%s: server2", indexes2[j].obj.schemaname,
					 indexes2[j].obj.objectname);

			dumpCreateIndex(fpre, &indexes2[j]);

			j++;
			qstat.indexadded++;
		}
	}

	freeIndexes(indexes1, nindexes1);
	freeIndexes(indexes2, nindexes2);
}

static void
quarrelLanguages()
{
	PQLLanguage		*languages1 = NULL;		/* target */
	PQLLanguage		*languages2 = NULL;		/* source */
	int			nlanguages1 = 0;		/* # of languages */
	int			nlanguages2 = 0;
	int			i, j;

	languages1 = getLanguages(conn1, &nlanguages1);
	languages2 = getLanguages(conn2, &nlanguages2);

	for (i = 0; i < nlanguages1; i++)
		logNoise("server1: %s", languages1[i].languagename);

	for (i = 0; i < nlanguages2; i++)
		logNoise("server2: %s", languages2[i].languagename);

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

			if (options.securitylabels)
				getLanguageSecurityLabels(conn2, &languages2[j]);

			dumpCreateLanguage(fpre, &languages2[j]);

			j++;
			qstat.languageadded++;
		}
		/* End of languages2 list. Print languages1 list until its end. */
		else if (j == nlanguages2)
		{
			logDebug("language %s: server1", languages1[i].languagename);

			dumpDropLanguage(fpost, &languages1[i]);

			i++;
			qstat.languageremoved++;
		}
		else if (strcmp(languages1[i].languagename, languages2[j].languagename) == 0)
		{
			logDebug("language %s: server1 server2", languages1[i].languagename);

			if (options.securitylabels)
			{
				getLanguageSecurityLabels(conn1, &languages1[i]);
				getLanguageSecurityLabels(conn2, &languages2[j]);
			}

			dumpAlterLanguage(fpre, &languages1[i], &languages2[j]);

			i++;
			j++;
		}
		else if (strcmp(languages1[i].languagename, languages2[j].languagename) < 0)
		{
			logDebug("language %s: server1", languages1[i].languagename);

			dumpDropLanguage(fpost, &languages1[i]);

			i++;
			qstat.languageremoved++;
		}
		else if (strcmp(languages1[i].languagename, languages2[j].languagename) > 0)
		{
			logDebug("language %s: server2", languages2[j].languagename);

			if (options.securitylabels)
				getLanguageSecurityLabels(conn2, &languages2[j]);

			dumpCreateLanguage(fpre, &languages2[j]);

			j++;
			qstat.languageadded++;
		}
	}

	freeLanguages(languages1, nlanguages1);
	freeLanguages(languages2, nlanguages2);
}

static void
quarrelMaterializedViews()
{
	PQLMaterializedView	*matviews1 = NULL;	/* target */
	PQLMaterializedView	*matviews2 = NULL;	/* source */
	int			nmatviews1 = 0;		/* # of matviews */
	int			nmatviews2 = 0;
	int			i, j;

	matviews1 = getMaterializedViews(conn1, &nmatviews1);
	matviews2 = getMaterializedViews(conn2, &nmatviews2);

	for (i = 0; i < nmatviews1; i++)
		logNoise("server1: %s.%s", matviews1[i].obj.schemaname,
				 matviews1[i].obj.objectname);

	for (i = 0; i < nmatviews2; i++)
		logNoise("server2: %s.%s", matviews2[i].obj.schemaname,
				 matviews2[i].obj.objectname);

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
			if (options.securitylabels)
				getMaterializedViewSecurityLabels(conn2, &matviews2[j]);

			dumpCreateMaterializedView(fpre, &matviews2[j]);

			j++;
			qstat.matviewadded++;
		}
		/* End of matviews2 list. Print matviews1 list until its end. */
		else if (j == nmatviews2)
		{
			logDebug("materialized view %s.%s: server1", matviews1[i].obj.schemaname,
					 matviews1[i].obj.objectname);

			dumpDropMaterializedView(fpost, &matviews1[i]);

			i++;
			qstat.matviewremoved++;
		}
		else if (compareRelations(&matviews1[i].obj, &matviews2[j].obj) == 0)
		{
			logDebug("materialized view %s.%s: server1 server2",
					 matviews1[i].obj.schemaname,
					 matviews1[i].obj.objectname);

			getMaterializedViewAttributes(conn1, &matviews1[i]);
			getMaterializedViewAttributes(conn2, &matviews2[j]);
			if (options.securitylabels)
			{
				getMaterializedViewSecurityLabels(conn1, &matviews1[i]);
				getMaterializedViewSecurityLabels(conn2, &matviews2[j]);
			}

			dumpAlterMaterializedView(fpre, &matviews1[i], &matviews2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&matviews1[i].obj, &matviews2[j].obj) < 0)
		{
			logDebug("materialized view %s.%s: server1", matviews1[i].obj.schemaname,
					 matviews1[i].obj.objectname);

			dumpDropMaterializedView(fpost, &matviews1[i]);

			i++;
			qstat.matviewremoved++;
		}
		else if (compareRelations(&matviews1[i].obj, &matviews2[j].obj) > 0)
		{
			logDebug("materialized view %s.%s: server2", matviews2[j].obj.schemaname,
					 matviews2[j].obj.objectname);

			getMaterializedViewAttributes(conn2, &matviews2[j]);
			if (options.securitylabels)
				getMaterializedViewSecurityLabels(conn2, &matviews2[j]);

			dumpCreateMaterializedView(fpre, &matviews2[j]);

			j++;
			qstat.matviewadded++;
		}
	}

	freeMaterializedViews(matviews1, nmatviews1);
	freeMaterializedViews(matviews2, nmatviews2);
}

static void
quarrelOperators()
{
	PQLOperator	*operators1 = NULL;		/* target */
	PQLOperator	*operators2 = NULL;		/* source */
	int			noperators1 = 0;		/* # of operators */
	int			noperators2 = 0;
	int			i, j;

	operators1 = getOperators(conn1, &noperators1);
	operators2 = getOperators(conn2, &noperators2);

	for (i = 0; i < noperators1; i++)
		logNoise("server1: %s.%s", operators1[i].obj.schemaname,
				 operators1[i].obj.objectname);

	for (i = 0; i < noperators2; i++)
		logNoise("server2: %s.%s", operators2[i].obj.schemaname,
				 operators2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < noperators1 || j < noperators2)
	{
		/* End of operators1 list. Print operators2 list until its end. */
		if (i == noperators1)
		{
			logDebug("operator %s.%s: server2", operators2[j].obj.schemaname,
					 operators2[j].obj.objectname);

			dumpCreateOperator(fpre, &operators2[j]);

			j++;
			qstat.operatoradded++;
		}
		/* End of operators2 list. Print operators1 list until its end. */
		else if (j == noperators2)
		{
			logDebug("operator %s.%s: server1", operators1[i].obj.schemaname,
					 operators1[i].obj.objectname);

			dumpDropOperator(fpost, &operators1[i]);

			i++;
			qstat.operatorremoved++;
		}
		else if (compareOperators(&operators1[i], &operators2[j]) == 0)
		{
			logDebug("operator %s.%s: server1 server2", operators1[i].obj.schemaname,
					 operators1[i].obj.objectname);

			dumpAlterOperator(fpre, &operators1[i], &operators2[j]);

			i++;
			j++;
		}
		else if (compareOperators(&operators1[i], &operators2[j]) < 0)
		{
			logDebug("operator %s.%s: server1", operators1[i].obj.schemaname,
					 operators1[i].obj.objectname);

			dumpDropOperator(fpost, &operators1[i]);

			i++;
			qstat.operatorremoved++;
		}
		else if (compareOperators(&operators1[i], &operators2[j]) > 0)
		{
			logDebug("operator %s.%s: server2", operators2[j].obj.schemaname,
					 operators2[j].obj.objectname);

			dumpCreateOperator(fpre, &operators2[j]);

			j++;
			qstat.operatoradded++;
		}
	}

	freeOperators(operators1, noperators1);
	freeOperators(operators2, noperators2);
}

static void
quarrelOperatorFamilies()
{
	PQLOperatorFamily	*opfamilies1 = NULL;		/* target */
	PQLOperatorFamily	*opfamilies2 = NULL;		/* source */
	int			nopfamilies1 = 0;					/* # of opfamilies */
	int			nopfamilies2 = 0;
	int			i, j;

	opfamilies1 = getOperatorFamilies(conn1, &nopfamilies1);
	opfamilies2 = getOperatorFamilies(conn2, &nopfamilies2);

	for (i = 0; i < nopfamilies1; i++)
		logNoise("server1: %s.%s", opfamilies1[i].obj.schemaname,
				 opfamilies1[i].obj.objectname);

	for (i = 0; i < nopfamilies2; i++)
		logNoise("server2: %s.%s", opfamilies2[i].obj.schemaname,
				 opfamilies2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nopfamilies1 || j < nopfamilies2)
	{
		/* End of opfamilies1 list. Print opfamilies2 list until its end. */
		if (i == nopfamilies1)
		{
			logDebug("operator family %s.%s: server2", opfamilies2[j].obj.schemaname,
					 opfamilies2[j].obj.objectname);

			dumpCreateOperatorFamily(fpre, &opfamilies2[j]);

			j++;
			qstat.opfamilyadded++;
		}
		/* End of opfamilies2 list. Print opfamilies1 list until its end. */
		else if (j == nopfamilies2)
		{
			logDebug("operator family %s.%s: server1", opfamilies1[i].obj.schemaname,
					 opfamilies1[i].obj.objectname);

			dumpDropOperatorFamily(fpost, &opfamilies1[i]);

			i++;
			qstat.opfamilyremoved++;
		}
		else if (compareRelations(&opfamilies1[i].obj, &opfamilies2[j].obj) == 0)
		{
			logDebug("operator family %s.%s: server1 server2",
					 opfamilies1[i].obj.schemaname,
					 opfamilies1[i].obj.objectname);

			dumpAlterOperatorFamily(fpre, &opfamilies1[i], &opfamilies2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&opfamilies1[i].obj, &opfamilies2[j].obj) < 0)
		{
			logDebug("operator family %s.%s: server1", opfamilies1[i].obj.schemaname,
					 opfamilies1[i].obj.objectname);

			dumpDropOperatorFamily(fpost, &opfamilies1[i]);

			i++;
			qstat.opfamilyremoved++;
		}
		else if (compareRelations(&opfamilies1[i].obj, &opfamilies2[j].obj) > 0)
		{
			logDebug("operator family %s.%s: server2", opfamilies2[j].obj.schemaname,
					 opfamilies2[j].obj.objectname);

			dumpCreateOperatorFamily(fpre, &opfamilies2[j]);

			j++;
			qstat.opfamilyadded++;
		}
	}

	freeOperatorFamilies(opfamilies1, nopfamilies1);
	freeOperatorFamilies(opfamilies2, nopfamilies2);
}

static void
quarrelOperatorClasses()
{
	PQLOperatorClass	*opclasses1 = NULL;		/* target */
	PQLOperatorClass	*opclasses2 = NULL;		/* source */
	int			nopclasses1 = 0;					/* # of opclasses */
	int			nopclasses2 = 0;
	int			i, j;

	opclasses1 = getOperatorClasses(conn1, &nopclasses1);
	opclasses2 = getOperatorClasses(conn2, &nopclasses2);

	for (i = 0; i < nopclasses1; i++)
		logNoise("server1: %s.%s", opclasses1[i].obj.schemaname,
				 opclasses1[i].obj.objectname);

	for (i = 0; i < nopclasses2; i++)
		logNoise("server2: %s.%s", opclasses2[i].obj.schemaname,
				 opclasses2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nopclasses1 || j < nopclasses2)
	{
		/* End of opclasses1 list. Print opclasses2 list until its end. */
		if (i == nopclasses1)
		{
			logDebug("operator class %s.%s: server2", opclasses2[j].obj.schemaname,
					 opclasses2[j].obj.objectname);

			dumpCreateOperatorClass(fpre, &opclasses2[j]);

			j++;
			qstat.opclassadded++;
		}
		/* End of opclasses2 list. Print opclasses1 list until its end. */
		else if (j == nopclasses2)
		{
			logDebug("operator class %s.%s: server1", opclasses1[i].obj.schemaname,
					 opclasses1[i].obj.objectname);

			dumpDropOperatorClass(fpost, &opclasses1[i]);

			i++;
			qstat.opclassremoved++;
		}
		else if (compareRelations(&opclasses1[i].obj, &opclasses2[j].obj) == 0)
		{
			logDebug("operator class %s.%s: server1 server2", opclasses1[i].obj.schemaname,
					 opclasses1[i].obj.objectname);

			dumpAlterOperatorClass(fpre, &opclasses1[i], &opclasses2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&opclasses1[i].obj, &opclasses2[j].obj) < 0)
		{
			logDebug("operator class %s.%s: server1", opclasses1[i].obj.schemaname,
					 opclasses1[i].obj.objectname);

			dumpDropOperatorClass(fpost, &opclasses1[i]);

			i++;
			qstat.opclassremoved++;
		}
		else if (compareRelations(&opclasses1[i].obj, &opclasses2[j].obj) > 0)
		{
			logDebug("operator class %s.%s: server2", opclasses2[j].obj.schemaname,
					 opclasses2[j].obj.objectname);

			dumpCreateOperatorClass(fpre, &opclasses2[j]);

			j++;
			qstat.opclassadded++;
		}
	}

	freeOperatorClasses(opclasses1, nopclasses1);
	freeOperatorClasses(opclasses2, nopclasses2);
}

static void
quarrelProcedures()
{
	PQLFunction	*procedures1 = NULL;	/* target */
	PQLFunction	*procedures2 = NULL;	/* source */
	int			nprocedures1 = 0;		/* # of procedures */
	int			nprocedures2 = 0;
	int			i, j;

	procedures1 = getProcedures(conn1, &nprocedures1);
	procedures2 = getProcedures(conn2, &nprocedures2);

	for (i = 0; i < nprocedures1; i++)
		logNoise("server1: %s.%s(%s) %s", procedures1[i].obj.schemaname,
				 procedures1[i].obj.objectname, procedures1[i].arguments,
				 procedures1[i].returntype);

	for (i = 0; i < nprocedures2; i++)
		logNoise("server2: %s.%s(%s) %s", procedures2[i].obj.schemaname,
				 procedures2[i].obj.objectname, procedures2[i].arguments,
				 procedures2[i].returntype);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nprocedures1 || j < nprocedures2)
	{
		/* End of procedures1 list. Print procedures2 list until its end. */
		if (i == nprocedures1)
		{
			logDebug("procedure %s.%s(%s): server2", procedures2[j].obj.schemaname,
					 procedures2[j].obj.objectname, procedures2[j].arguments);

			if (options.securitylabels)
				getProcedureSecurityLabels(conn2, &procedures2[j]);

			dumpCreateProcedure(fpre, &procedures2[j], false);

			j++;
			qstat.procadded++;
		}
		/* End of procedures2 list. Print procedures1 list until its end. */
		else if (j == nprocedures2)
		{
			logDebug("procedure %s.%s(%s): server1", procedures1[i].obj.schemaname,
					 procedures1[i].obj.objectname, procedures1[i].arguments);

			dumpDropProcedure(fpost, &procedures1[i]);

			i++;
			qstat.procremoved++;
		}
		else if (compareFunctions(&procedures1[i], &procedures2[j]) == 0)
		{
			logDebug("procedure %s.%s(%s): server1 server2", procedures1[i].obj.schemaname,
					 procedures1[i].obj.objectname, procedures1[i].arguments);

			if (options.securitylabels)
			{
				getProcedureSecurityLabels(conn1, &procedures1[i]);
				getProcedureSecurityLabels(conn2, &procedures2[j]);
			}

			/*
			 * When we change return type we have to recreate the procedure
			 * because there is no ALTER FUNCTION command for it.
			 */
			if (strcmp(procedures1[i].returntype, procedures2[j].returntype) == 0)
				dumpAlterProcedure(fpre, &procedures1[i], &procedures2[j]);
			else
			{
				dumpDropProcedure(fpre, &procedures1[i]);
				dumpCreateProcedure(fpre, &procedures2[j], false);
			}

			i++;
			j++;
		}
		else if (compareFunctions(&procedures1[i], &procedures2[j]) < 0)
		{
			logDebug("procedure %s.%s(%s): server1", procedures1[i].obj.schemaname,
					 procedures1[i].obj.objectname, procedures1[i].arguments);

			dumpDropProcedure(fpost, &procedures1[i]);

			i++;
			qstat.procremoved++;
		}
		else if (compareFunctions(&procedures1[i], &procedures2[j]) > 0)
		{
			logDebug("procedure %s.%s(%s): server2", procedures2[j].obj.schemaname,
					 procedures2[j].obj.objectname, procedures2[j].arguments);

			if (options.securitylabels)
				getProcedureSecurityLabels(conn2, &procedures2[j]);

			dumpCreateProcedure(fpre, &procedures2[j], false);

			j++;
			qstat.procadded++;
		}
	}

	freeFunctions(procedures1, nprocedures1);
	freeFunctions(procedures2, nprocedures2);
}

static void
quarrelPublications()
{
	PQLPublication		*publications1 = NULL;		/* target */
	PQLPublication		*publications2 = NULL;		/* source */
	int			npublications1 = 0;		/* # of publications */
	int			npublications2 = 0;
	int			i, j;

	publications1 = getPublications(conn1, &npublications1);
	publications2 = getPublications(conn2, &npublications2);

	for (i = 0; i < npublications1; i++)
		logNoise("server1: %s", publications1[i].pubname);

	for (i = 0; i < npublications2; i++)
		logNoise("server2: %s", publications2[i].pubname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out publications not presented in the other list.
	 */
	i = j = 0;
	while (i < npublications1 || j < npublications2)
	{
		/* End of publications1 list. Print publications2 list until its end. */
		if (i == npublications1)
		{
			logDebug("publication %s: server2", publications2[j].pubname);

			getPublicationTables(conn2, &publications2[j]);
			if (options.securitylabels)
				getPublicationSecurityLabels(conn2, &publications2[j]);

			dumpCreatePublication(fpre, &publications2[j]);

			j++;
			qstat.pubadded++;
		}
		/* End of publications2 list. Print publications1 list until its end. */
		else if (j == npublications2)
		{
			logDebug("publication %s: server1", publications1[i].pubname);

			dumpDropPublication(fpost, &publications1[i]);

			i++;
			qstat.pubremoved++;
		}
		else if (strcmp(publications1[i].pubname, publications2[j].pubname) == 0)
		{
			logDebug("publication %s: server1 server2", publications1[i].pubname);

			getPublicationTables(conn1, &publications1[i]);
			getPublicationTables(conn2, &publications2[j]);
			if (options.securitylabels)
			{
				getPublicationSecurityLabels(conn1, &publications1[i]);
				getPublicationSecurityLabels(conn2, &publications2[j]);
			}

			dumpAlterPublication(fpre, &publications1[i], &publications2[j]);

			i++;
			j++;
		}
		else if (strcmp(publications1[i].pubname, publications2[j].pubname) < 0)
		{
			logDebug("publication %s: server1", publications1[i].pubname);

			dumpDropPublication(fpost, &publications1[i]);

			i++;
			qstat.pubremoved++;
		}
		else if (strcmp(publications1[i].pubname, publications2[j].pubname) > 0)
		{
			logDebug("publication %s: server2", publications2[j].pubname);

			getPublicationTables(conn2, &publications2[j]);
			if (options.securitylabels)
				getPublicationSecurityLabels(conn2, &publications2[j]);

			dumpCreatePublication(fpre, &publications2[j]);

			j++;
			qstat.pubadded++;
		}
	}

	freePublications(publications1, npublications1);
	freePublications(publications2, npublications2);
}

static void
quarrelPolicies()
{
	PQLPolicy	*policies1 = NULL;	/* target */
	PQLPolicy	*policies2 = NULL;	/* source */
	int			npolicies1 = 0;		/* # of policies */
	int			npolicies2 = 0;
	int			i, j;

	policies1 = getPolicies(conn1, &npolicies1);
	policies2 = getPolicies(conn2, &npolicies2);

	for (i = 0; i < npolicies1; i++)
		logNoise("server1: %s.%s", policies1[i].table.schemaname,
				 policies1[i].table.objectname);

	for (i = 0; i < npolicies2; i++)
		logNoise("server2: %s.%s", policies2[i].table.schemaname,
				 policies2[i].table.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < npolicies1 || j < npolicies2)
	{
		/* End of policies1 list. Print policies2 list until its end. */
		if (i == npolicies1)
		{
			logDebug("policy %s.%s: server2", policies2[j].table.schemaname,
					 policies2[j].table.objectname);

			dumpCreatePolicy(fpre, &policies2[j]);

			j++;
			qstat.poladded++;
		}
		/* End of policies2 list. Print policies1 list until its end. */
		else if (j == npolicies2)
		{
			logDebug("policy %s.%s: server1", policies1[i].table.schemaname,
					 policies1[i].table.objectname);

			dumpDropPolicy(fpost, &policies1[i]);

			i++;
			qstat.polremoved++;
		}
		else if (compareNamesAndRelations(&policies1[i].table, &policies2[j].table,
										  policies1[i].polname, policies2[j].polname) == 0)
		{
			logDebug("policy %s.%s: server1 server2", policies1[i].table.schemaname,
					 policies1[i].table.objectname);

			dumpAlterPolicy(fpre, &policies1[i], &policies2[j]);

			i++;
			j++;
		}
		else if (compareNamesAndRelations(&policies1[i].table, &policies2[j].table,
										  policies1[i].polname, policies2[j].polname) < 0)
		{
			logDebug("policy %s.%s: server1", policies1[i].table.schemaname,
					 policies1[i].table.objectname);

			dumpDropPolicy(fpost, &policies1[i]);

			i++;
			qstat.polremoved++;
		}
		else if (compareNamesAndRelations(&policies1[i].table, &policies2[j].table,
										  policies1[i].polname, policies2[j].polname) > 0)
		{
			logDebug("policy %s.%s: server2", policies2[j].table.schemaname,
					 policies2[j].table.objectname);

			dumpCreatePolicy(fpre, &policies2[j]);

			j++;
			qstat.poladded++;
		}
	}

	freePolicies(policies1, npolicies1);
	freePolicies(policies2, npolicies2);
}

static void
quarrelRules()
{
	PQLRule	*rules1 = NULL;	/* target */
	PQLRule	*rules2 = NULL;	/* source */
	int			nrules1 = 0;		/* # of rules */
	int			nrules2 = 0;
	int			i, j;

	rules1 = getRules(conn1, &nrules1);
	rules2 = getRules(conn2, &nrules2);

	for (i = 0; i < nrules1; i++)
		logNoise("server1: %s.%s", rules1[i].table.schemaname,
				 rules1[i].table.objectname);

	for (i = 0; i < nrules2; i++)
		logNoise("server2: %s.%s", rules2[i].table.schemaname,
				 rules2[i].table.objectname);

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

			dumpCreateRule(fpre, &rules2[j]);

			j++;
			qstat.ruleadded++;
		}
		/* End of rules2 list. Print rules1 list until its end. */
		else if (j == nrules2)
		{
			logDebug("rule %s.%s: server1", rules1[i].table.schemaname,
					 rules1[i].table.objectname);

			dumpDropRule(fpost, &rules1[i]);

			i++;
			qstat.ruleremoved++;
		}
		else if (compareNamesAndRelations(&rules1[i].table, &rules2[j].table,
										  rules1[i].rulename, rules2[j].rulename) == 0)
		{
			logDebug("rule %s.%s: server1 server2", rules1[i].table.schemaname,
					 rules1[i].table.objectname);

			dumpAlterRule(fpre, &rules1[i], &rules2[j]);

			i++;
			j++;
		}
		else if (compareNamesAndRelations(&rules1[i].table, &rules2[j].table,
										  rules1[i].rulename, rules2[j].rulename) < 0)
		{
			logDebug("rule %s.%s: server1", rules1[i].table.schemaname,
					 rules1[i].table.objectname);

			dumpDropRule(fpost, &rules1[i]);

			i++;
			qstat.ruleremoved++;
		}
		else if (compareNamesAndRelations(&rules1[i].table, &rules2[j].table,
										  rules1[i].rulename, rules2[j].rulename) > 0)
		{
			logDebug("rule %s.%s: server2", rules2[j].table.schemaname,
					 rules2[j].table.objectname);

			dumpCreateRule(fpre, &rules2[j]);

			j++;
			qstat.ruleadded++;
		}
	}

	freeRules(rules1, nrules1);
	freeRules(rules2, nrules2);
}

static void
quarrelSchemas()
{
	PQLSchema		*schemas1 = NULL;		/* target */
	PQLSchema		*schemas2 = NULL;		/* source */
	int			nschemas1 = 0;		/* # of schemas */
	int			nschemas2 = 0;
	int			i, j;

	schemas1 = getSchemas(conn1, &nschemas1);
	schemas2 = getSchemas(conn2, &nschemas2);

	for (i = 0; i < nschemas1; i++)
		logNoise("server1: %s", schemas1[i].schemaname);

	for (i = 0; i < nschemas2; i++)
		logNoise("server2: %s", schemas2[i].schemaname);

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

			if (options.securitylabels)
				getSchemaSecurityLabels(conn2, &schemas2[j]);

			dumpCreateSchema(fpre, &schemas2[j]);

			j++;
			qstat.schemaadded++;
		}
		/* End of schemas2 list. Print schemas1 list until its end. */
		else if (j == nschemas2)
		{
			logDebug("schema %s: server1", schemas1[i].schemaname);

			dumpDropSchema(fpost, &schemas1[i]);

			i++;
			qstat.schemaremoved++;
		}
		else if (strcmp(schemas1[i].schemaname, schemas2[j].schemaname) == 0)
		{
			logDebug("schema %s: server1 server2", schemas1[i].schemaname);

			if (options.securitylabels)
			{
				getSchemaSecurityLabels(conn1, &schemas1[i]);
				getSchemaSecurityLabels(conn2, &schemas2[j]);
			}

			dumpAlterSchema(fpre, &schemas1[i], &schemas2[j]);

			i++;
			j++;
		}
		else if (strcmp(schemas1[i].schemaname, schemas2[j].schemaname) < 0)
		{
			logDebug("schema %s: server1", schemas1[i].schemaname);

			dumpDropSchema(fpost, &schemas1[i]);

			i++;
			qstat.schemaremoved++;
		}
		else if (strcmp(schemas1[i].schemaname, schemas2[j].schemaname) > 0)
		{
			logDebug("schema %s: server2", schemas2[j].schemaname);

			if (options.securitylabels)
				getSchemaSecurityLabels(conn2, &schemas2[j]);

			dumpCreateSchema(fpre, &schemas2[j]);

			j++;
			qstat.schemaadded++;
		}
	}

	freeSchemas(schemas1, nschemas1);
	freeSchemas(schemas2, nschemas2);
}

static void
quarrelSequences()
{
	PQLSequence	*sequences1 = NULL;	/* target */
	PQLSequence	*sequences2 = NULL;	/* source */
	int			nsequences1 = 0;	/* # of sequences */
	int			nsequences2 = 0;
	int			i, j;

	sequences1 = getSequences(conn1, &nsequences1);
	sequences2 = getSequences(conn2, &nsequences2);

	for (i = 0; i < nsequences1; i++)
		logNoise("server1: %s.%s", sequences1[i].obj.schemaname,
				 sequences1[i].obj.objectname);

	for (i = 0; i < nsequences2; i++)
		logNoise("server2: %s.%s", sequences2[i].obj.schemaname,
				 sequences2[i].obj.objectname);

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
			if (options.securitylabels)
				getSequenceSecurityLabels(conn2, &sequences2[j]);

			dumpCreateSequence(fpre, &sequences2[j]);

			j++;
			qstat.seqadded++;
		}
		/* End of sequences2 list. Print sequences1 list until its end. */
		else if (j == nsequences2)
		{
			logDebug("sequence %s.%s: server1", sequences1[i].obj.schemaname,
					 sequences1[i].obj.objectname);

			dumpDropSequence(fpost, &sequences1[i]);

			i++;
			qstat.seqremoved++;
		}
		else if (compareRelations(&sequences1[i].obj, &sequences2[j].obj) == 0)
		{
			logDebug("sequence %s.%s: server1 server2", sequences1[i].obj.schemaname,
					 sequences1[i].obj.objectname);

			getSequenceAttributes(conn1, &sequences1[i]);
			getSequenceAttributes(conn2, &sequences2[j]);
			if (options.securitylabels)
			{
				getSequenceSecurityLabels(conn1, &sequences1[i]);
				getSequenceSecurityLabels(conn2, &sequences2[j]);
			}

			dumpAlterSequence(fpre, &sequences1[i], &sequences2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&sequences1[i].obj, &sequences2[j].obj) < 0)
		{
			logDebug("sequence %s.%s: server1", sequences1[i].obj.schemaname,
					 sequences1[i].obj.objectname);

			dumpDropSequence(fpost, &sequences1[i]);

			i++;
			qstat.seqremoved++;
		}
		else if (compareRelations(&sequences1[i].obj, &sequences2[j].obj) > 0)
		{
			logDebug("sequence %s.%s: server2", sequences2[j].obj.schemaname,
					 sequences2[j].obj.objectname);

			getSequenceAttributes(conn2, &sequences2[j]);
			if (options.securitylabels)
				getSequenceSecurityLabels(conn2, &sequences2[j]);

			dumpCreateSequence(fpre, &sequences2[j]);

			j++;
			qstat.seqadded++;
		}
	}

	freeSequences(sequences1, nsequences1);
	freeSequences(sequences2, nsequences2);
}

static void
quarrelStatistics()
{
	PQLStatistics	*statistics1 = NULL;	/* target */
	PQLStatistics	*statistics2 = NULL;	/* source */
	int				nstatistics1 = 0;		/* # of statistics */
	int				nstatistics2 = 0;
	int				i, j;

	statistics1 = getStatistics(conn1, &nstatistics1);
	statistics2 = getStatistics(conn2, &nstatistics2);

	for (i = 0; i < nstatistics1; i++)
		logNoise("server1: %s.%s", statistics1[i].obj.schemaname,
				 statistics1[i].obj.objectname);

	for (i = 0; i < nstatistics2; i++)
		logNoise("server2: %s.%s", statistics2[i].obj.schemaname,
				 statistics2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out relations not presented in the other list.
	 */
	i = j = 0;
	while (i < nstatistics1 || j < nstatistics2)
	{
		/* End of statistics1 list. Print statistics2 list until its end. */
		if (i == nstatistics1)
		{
			logDebug("statistics %s.%s: server2", statistics2[j].obj.schemaname,
					 statistics2[j].obj.objectname);

			dumpCreateStatistics(fpre, &statistics2[j]);

			j++;
			qstat.stxadded++;
		}
		/* End of statistics2 list. Print statistics1 list until its end. */
		else if (j == nstatistics2)
		{
			logDebug("statistics %s.%s: server1", statistics1[i].obj.schemaname,
					 statistics1[i].obj.objectname);

			dumpDropStatistics(fpost, &statistics1[i]);

			i++;
			qstat.stxremoved++;
		}
		else if (compareRelations(&statistics1[i].obj, &statistics2[j].obj) == 0)
		{
			logDebug("statistics %s.%s: server1 server2", statistics1[i].obj.schemaname,
					 statistics1[i].obj.objectname);

			dumpAlterStatistics(fpre, &statistics1[i], &statistics2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&statistics1[i].obj, &statistics2[j].obj) < 0)
		{
			logDebug("statistics %s.%s: server1", statistics1[i].obj.schemaname,
					 statistics1[i].obj.objectname);

			dumpDropStatistics(fpost, &statistics1[i]);

			i++;
			qstat.stxremoved++;
		}
		else if (compareRelations(&statistics1[i].obj, &statistics2[j].obj) > 0)
		{
			logDebug("statistics %s.%s: server2", statistics2[j].obj.schemaname,
					 statistics2[j].obj.objectname);

			dumpCreateStatistics(fpre, &statistics2[j]);

			j++;
			qstat.stxadded++;
		}
	}

	freeStatistics(statistics1, nstatistics1);
	freeStatistics(statistics2, nstatistics2);
}

static void
quarrelSubscriptions()
{
	PQLSubscription		*subscriptions1 = NULL;		/* target */
	PQLSubscription		*subscriptions2 = NULL;		/* source */
	int			nsubscriptions1 = 0;		/* # of subscriptions */
	int			nsubscriptions2 = 0;
	int			i, j;

	subscriptions1 = getSubscriptions(conn1, &nsubscriptions1);
	subscriptions2 = getSubscriptions(conn2, &nsubscriptions2);

	for (i = 0; i < nsubscriptions1; i++)
		logNoise("server1: %s", subscriptions1[i].subname);

	for (i = 0; i < nsubscriptions2; i++)
		logNoise("server2: %s", subscriptions2[i].subname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out subscriptions not presented in the other list.
	 */
	i = j = 0;
	while (i < nsubscriptions1 || j < nsubscriptions2)
	{
		/* End of subscriptions1 list. Print subscriptions2 list until its end. */
		if (i == nsubscriptions1)
		{
			logDebug("subscription %s: server2", subscriptions2[j].subname);

			getSubscriptionPublications(conn2, &subscriptions2[j]);
			if (options.securitylabels)
				getSubscriptionSecurityLabels(conn2, &subscriptions2[j]);

			dumpCreateSubscription(fpre, &subscriptions2[j]);

			j++;
			qstat.subadded++;
		}
		/* End of subscriptions2 list. Print subscriptions1 list until its end. */
		else if (j == nsubscriptions2)
		{
			logDebug("subscription %s: server1", subscriptions1[i].subname);

			dumpDropSubscription(fpost, &subscriptions1[i]);

			i++;
			qstat.subremoved++;
		}
		else if (strcmp(subscriptions1[i].subname, subscriptions2[j].subname) == 0)
		{
			logDebug("subscription %s: server1 server2", subscriptions1[i].subname);

			getSubscriptionPublications(conn1, &subscriptions1[i]);
			getSubscriptionPublications(conn2, &subscriptions2[j]);
			if (options.securitylabels)
			{
				getSubscriptionSecurityLabels(conn1, &subscriptions1[i]);
				getSubscriptionSecurityLabels(conn2, &subscriptions2[j]);
			}

			dumpAlterSubscription(fpre, &subscriptions1[i], &subscriptions2[j]);

			i++;
			j++;
		}
		else if (strcmp(subscriptions1[i].subname, subscriptions2[j].subname) < 0)
		{
			logDebug("subscription %s: server1", subscriptions1[i].subname);

			dumpDropSubscription(fpost, &subscriptions1[i]);

			i++;
			qstat.subremoved++;
		}
		else if (strcmp(subscriptions1[i].subname, subscriptions2[j].subname) > 0)
		{
			logDebug("subscription %s: server2", subscriptions2[j].subname);

			getSubscriptionPublications(conn2, &subscriptions2[j]);
			if (options.securitylabels)
				getSubscriptionSecurityLabels(conn2, &subscriptions2[j]);

			dumpCreateSubscription(fpre, &subscriptions2[j]);

			j++;
			qstat.subadded++;
		}
	}

	freeSubscriptions(subscriptions1, nsubscriptions1);
	freeSubscriptions(subscriptions2, nsubscriptions2);
}

static void
quarrelForeignTables()
{
	PQLTable	*tables1 = NULL;	/* target */
	PQLTable	*tables2 = NULL;	/* source */
	int			ntables1 = 0;		/* # of tables */
	int			ntables2 = 0;
	int			i, j;

	tables1 = getForeignTables(conn1, &ntables1);
	getForeignTableProperties(conn1, tables1, ntables1);
	getCheckConstraints(conn1, tables1, ntables1);

	tables2 = getForeignTables(conn2, &ntables2);
	getForeignTableProperties(conn2, tables2, ntables2);
	getCheckConstraints(conn2, tables2, ntables2);

	for (i = 0; i < ntables1; i++)
		logNoise("server1: %s.%s %u", tables1[i].obj.schemaname,
				 tables1[i].obj.objectname, tables1[i].obj.oid);

	for (i = 0; i < ntables2; i++)
		logNoise("server2: %s.%s %u", tables2[i].obj.schemaname,
				 tables2[i].obj.objectname, tables2[i].obj.oid);

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
			logDebug("foreign table %s.%s: server2", tables2[j].obj.schemaname,
					 tables2[j].obj.objectname);

			getTableAttributes(conn2, &tables2[j]);
			if (options.securitylabels)
				getTableSecurityLabels(conn2, &tables2[j]);

			dumpCreateTable(fpre, fpost, &tables2[j]);

			j++;
			qstat.ftableadded++;
		}
		/* End of tables2 list. Print tables1 list until its end. */
		else if (j == ntables2)
		{
			logDebug("foreign table %s.%s: server1", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			dumpDropTable(fpost, &tables1[i]);

			i++;
			qstat.ftableremoved++;
		}
		else if (compareRelations(&tables1[i].obj, &tables2[j].obj) == 0)
		{
			logDebug("foreign table %s.%s: server1 server2", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			getTableAttributes(conn1, &tables1[i]);
			getTableAttributes(conn2, &tables2[j]);
			if (options.securitylabels)
			{
				getTableSecurityLabels(conn1, &tables1[i]);
				getTableSecurityLabels(conn2, &tables2[j]);
			}

			dumpAlterTable(fpre, &tables1[i], &tables2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&tables1[i].obj, &tables2[j].obj) < 0)
		{
			logDebug("foreign table %s.%s: server1", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			dumpDropTable(fpost, &tables1[i]);

			i++;
			qstat.ftableremoved++;
		}
		else if (compareRelations(&tables1[i].obj, &tables2[j].obj) > 0)
		{
			logDebug("foreign table %s.%s: server2", tables2[j].obj.schemaname,
					 tables2[j].obj.objectname);

			getTableAttributes(conn2, &tables2[j]);
			if (options.securitylabels)
				getTableSecurityLabels(conn2, &tables2[j]);

			dumpCreateTable(fpre, fpost, &tables2[j]);

			j++;
			qstat.ftableadded++;
		}
	}

	freeTables(tables1, ntables1);
	freeTables(tables2, ntables2);
}

static void
quarrelTables()
{
	PQLTable	*tables1 = NULL;	/* target */
	PQLTable	*tables2 = NULL;	/* source */
	int			ntables1 = 0;		/* # of tables */
	int			ntables2 = 0;
	int			i, j;

	tables1 = getRegularTables(conn1, &ntables1);
	getCheckConstraints(conn1, tables1, ntables1);
	getFKConstraints(conn1, tables1, ntables1);
	getPKConstraints(conn1, tables1, ntables1);

	tables2 = getRegularTables(conn2, &ntables2);
	getCheckConstraints(conn2, tables2, ntables2);
	getFKConstraints(conn2, tables2, ntables2);
	getPKConstraints(conn2, tables2, ntables2);

	for (i = 0; i < ntables1; i++)
		logNoise("server1: %s.%s %u", tables1[i].obj.schemaname,
				 tables1[i].obj.objectname, tables1[i].obj.oid);

	for (i = 0; i < ntables2; i++)
		logNoise("server2: %s.%s %u", tables2[i].obj.schemaname,
				 tables2[i].obj.objectname, tables2[i].obj.oid);

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
			if (options.securitylabels)
				getTableSecurityLabels(conn2, &tables2[j]);

			dumpCreateTable(fpre, fpost, &tables2[j]);

			j++;
			qstat.tableadded++;
		}
		/* End of tables2 list. Print tables1 list until its end. */
		else if (j == ntables2)
		{
			logDebug("table %s.%s: server1", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			dumpDropTable(fpost, &tables1[i]);

			i++;
			qstat.tableremoved++;
		}
		else if (compareRelations(&tables1[i].obj, &tables2[j].obj) == 0)
		{
			logDebug("table %s.%s: server1 server2", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			getTableAttributes(conn1, &tables1[i]);
			getTableAttributes(conn2, &tables2[j]);
			getOwnedBySequences(conn1, &tables1[i]);
			getOwnedBySequences(conn2, &tables2[j]);
			if (options.securitylabels)
			{
				getTableSecurityLabels(conn1, &tables1[i]);
				getTableSecurityLabels(conn2, &tables2[j]);
			}

			dumpAlterTable(fpre, &tables1[i], &tables2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&tables1[i].obj, &tables2[j].obj) < 0)
		{
			logDebug("table %s.%s: server1", tables1[i].obj.schemaname,
					 tables1[i].obj.objectname);

			dumpDropTable(fpost, &tables1[i]);

			i++;
			qstat.tableremoved++;
		}
		else if (compareRelations(&tables1[i].obj, &tables2[j].obj) > 0)
		{
			logDebug("table %s.%s: server2", tables2[j].obj.schemaname,
					 tables2[j].obj.objectname);

			getTableAttributes(conn2, &tables2[j]);
			getOwnedBySequences(conn2, &tables2[j]);
			if (options.securitylabels)
				getTableSecurityLabels(conn2, &tables2[j]);

			dumpCreateTable(fpre, fpost, &tables2[j]);

			j++;
			qstat.tableadded++;
		}
	}

	freeTables(tables1, ntables1);
	freeTables(tables2, ntables2);
}

static void
quarrelTextSearchConfigs()
{
	PQLTextSearchConfig	*tsconfigs1 = NULL;	/* target */
	PQLTextSearchConfig	*tsconfigs2 = NULL;	/* source */
	int			ntsconfigs1 = 0;			/* # of text search configuration */
	int			ntsconfigs2 = 0;
	int			i, j;

	/* TextSearchConfigs */
	tsconfigs1 = getTextSearchConfigs(conn1, &ntsconfigs1);
	tsconfigs2 = getTextSearchConfigs(conn2, &ntsconfigs2);

	for (i = 0; i < ntsconfigs1; i++)
		logNoise("server1: %s.%s", tsconfigs1[i].obj.schemaname,
				 tsconfigs1[i].obj.objectname);

	for (i = 0; i < ntsconfigs2; i++)
		logNoise("server2: %s.%s", tsconfigs2[i].obj.schemaname,
				 tsconfigs2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out tsconfigs not presented in the other list.
	 */
	i = j = 0;
	while (i < ntsconfigs1 || j < ntsconfigs2)
	{
		/* End of tsconfigs1 list. Print tsconfigs2 list until its end. */
		if (i == ntsconfigs1)
		{
			logDebug("text search config %s.%s: server2", tsconfigs2[j].obj.schemaname,
					 tsconfigs2[j].obj.objectname);

			dumpCreateTextSearchConfig(fpre, &tsconfigs2[j]);

			j++;
			qstat.tsconfigadded++;
		}
		/* End of tsconfigs2 list. Print tsconfigs1 list until its end. */
		else if (j == ntsconfigs2)
		{
			logDebug("text search config %s.%s: server1", tsconfigs1[i].obj.schemaname,
					 tsconfigs1[i].obj.objectname);

			dumpDropTextSearchConfig(fpost, &tsconfigs1[i]);

			i++;
			qstat.tsconfigremoved++;
		}
		else if (compareRelations(&tsconfigs1[i].obj, &tsconfigs2[j].obj) == 0)
		{
			logDebug("text search config %s.%s: server1 server2",
					 tsconfigs1[i].obj.schemaname,
					 tsconfigs1[i].obj.objectname);

			dumpAlterTextSearchConfig(fpre, &tsconfigs1[i], &tsconfigs2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&tsconfigs1[i].obj, &tsconfigs2[j].obj) < 0)
		{
			logDebug("text search config %s.%s: server1", tsconfigs1[i].obj.schemaname,
					 tsconfigs1[i].obj.objectname);

			dumpDropTextSearchConfig(fpost, &tsconfigs1[i]);

			i++;
			qstat.tsconfigremoved++;
		}
		else if (compareRelations(&tsconfigs1[i].obj, &tsconfigs2[j].obj) > 0)
		{
			logDebug("text search config %s.%s: server2", tsconfigs2[j].obj.schemaname,
					 tsconfigs2[j].obj.objectname);

			dumpCreateTextSearchConfig(fpre, &tsconfigs2[j]);

			j++;
			qstat.tsconfigadded++;
		}
	}

	freeTextSearchConfigs(tsconfigs1, ntsconfigs1);
	freeTextSearchConfigs(tsconfigs2, ntsconfigs2);
}

static void
quarrelTextSearchDicts()
{
	PQLTextSearchDict	*tsdicts1 = NULL;	/* target */
	PQLTextSearchDict	*tsdicts2 = NULL;	/* source */
	int			ntsdicts1 = 0;				/* # of text search dictionary */
	int			ntsdicts2 = 0;
	int			i, j;

	/* TextSearchDicts */
	tsdicts1 = getTextSearchDicts(conn1, &ntsdicts1);
	tsdicts2 = getTextSearchDicts(conn2, &ntsdicts2);

	for (i = 0; i < ntsdicts1; i++)
		logNoise("server1: %s.%s", tsdicts1[i].obj.schemaname,
				 tsdicts1[i].obj.objectname);

	for (i = 0; i < ntsdicts2; i++)
		logNoise("server2: %s.%s", tsdicts2[i].obj.schemaname,
				 tsdicts2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out tsdicts not presented in the other list.
	 */
	i = j = 0;
	while (i < ntsdicts1 || j < ntsdicts2)
	{
		/* End of tsdicts1 list. Print tsdicts2 list until its end. */
		if (i == ntsdicts1)
		{
			logDebug("text search dictionary %s.%s: server2", tsdicts2[j].obj.schemaname,
					 tsdicts2[j].obj.objectname);

			dumpCreateTextSearchDict(fpre, &tsdicts2[j]);

			j++;
			qstat.tsdictadded++;
		}
		/* End of tsdicts2 list. Print tsdicts1 list until its end. */
		else if (j == ntsdicts2)
		{
			logDebug("text search dictionary %s.%s: server1", tsdicts1[i].obj.schemaname,
					 tsdicts1[i].obj.objectname);

			dumpDropTextSearchDict(fpost, &tsdicts1[i]);

			i++;
			qstat.tsdictremoved++;
		}
		else if (compareRelations(&tsdicts1[i].obj, &tsdicts2[j].obj) == 0)
		{
			logDebug("text search dictionary %s.%s: server1 server2",
					 tsdicts1[i].obj.schemaname,
					 tsdicts1[i].obj.objectname);

			dumpAlterTextSearchDict(fpre, &tsdicts1[i], &tsdicts2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&tsdicts1[i].obj, &tsdicts2[j].obj) < 0)
		{
			logDebug("text search dictionary %s.%s: server1", tsdicts1[i].obj.schemaname,
					 tsdicts1[i].obj.objectname);

			dumpDropTextSearchDict(fpost, &tsdicts1[i]);

			i++;
			qstat.tsdictremoved++;
		}
		else if (compareRelations(&tsdicts1[i].obj, &tsdicts2[j].obj) > 0)
		{
			logDebug("text search dictionary %s.%s: server2", tsdicts2[j].obj.schemaname,
					 tsdicts2[j].obj.objectname);

			dumpCreateTextSearchDict(fpre, &tsdicts2[j]);

			j++;
			qstat.tsdictadded++;
		}
	}

	freeTextSearchDicts(tsdicts1, ntsdicts1);
	freeTextSearchDicts(tsdicts2, ntsdicts2);
}

static void
quarrelTextSearchParsers()
{
	PQLTextSearchParser	*tsparsers1 = NULL;	/* target */
	PQLTextSearchParser	*tsparsers2 = NULL;	/* source */
	int			ntsparsers1 = 0;			/* # of text search parser */
	int			ntsparsers2 = 0;
	int			i, j;

	/* TextSearchParsers */
	tsparsers1 = getTextSearchParsers(conn1, &ntsparsers1);
	tsparsers2 = getTextSearchParsers(conn2, &ntsparsers2);

	for (i = 0; i < ntsparsers1; i++)
		logNoise("server1: %s.%s", tsparsers1[i].obj.schemaname,
				 tsparsers1[i].obj.objectname);

	for (i = 0; i < ntsparsers2; i++)
		logNoise("server2: %s.%s", tsparsers2[i].obj.schemaname,
				 tsparsers2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out tsparsers not presented in the other list.
	 */
	i = j = 0;
	while (i < ntsparsers1 || j < ntsparsers2)
	{
		/* End of tsparsers1 list. Print tsparsers2 list until its end. */
		if (i == ntsparsers1)
		{
			logDebug("text search parser %s.%s: server2", tsparsers2[j].obj.schemaname,
					 tsparsers2[j].obj.objectname);

			dumpCreateTextSearchParser(fpre, &tsparsers2[j]);

			j++;
			qstat.tsparseradded++;
		}
		/* End of tsparsers2 list. Print tsparsers1 list until its end. */
		else if (j == ntsparsers2)
		{
			logDebug("text search parser %s.%s: server1", tsparsers1[i].obj.schemaname,
					 tsparsers1[i].obj.objectname);

			dumpDropTextSearchParser(fpost, &tsparsers1[i]);

			i++;
			qstat.tsparserremoved++;
		}
		else if (compareRelations(&tsparsers1[i].obj, &tsparsers2[j].obj) == 0)
		{
			logDebug("text search parser %s.%s: server1 server2",
					 tsparsers1[i].obj.schemaname,
					 tsparsers1[i].obj.objectname);

			dumpAlterTextSearchParser(fpre, &tsparsers1[i], &tsparsers2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&tsparsers1[i].obj, &tsparsers2[j].obj) < 0)
		{
			logDebug("text search parser %s.%s: server1", tsparsers1[i].obj.schemaname,
					 tsparsers1[i].obj.objectname);

			dumpDropTextSearchParser(fpost, &tsparsers1[i]);

			i++;
			qstat.tsparserremoved++;
		}
		else if (compareRelations(&tsparsers1[i].obj, &tsparsers2[j].obj) > 0)
		{
			logDebug("text search parser %s.%s: server2", tsparsers2[j].obj.schemaname,
					 tsparsers2[j].obj.objectname);

			dumpCreateTextSearchParser(fpre, &tsparsers2[j]);

			j++;
			qstat.tsparseradded++;
		}
	}

	freeTextSearchParsers(tsparsers1, ntsparsers1);
	freeTextSearchParsers(tsparsers2, ntsparsers2);
}

static void
quarrelTextSearchTemplates()
{
	PQLTextSearchTemplate	*tstemplates1 = NULL;	/* target */
	PQLTextSearchTemplate	*tstemplates2 = NULL;	/* source */
	int			ntstemplates1 = 0;					/* # of text search template */
	int			ntstemplates2 = 0;
	int			i, j;

	/* TextSearchTemplates */
	tstemplates1 = getTextSearchTemplates(conn1, &ntstemplates1);
	tstemplates2 = getTextSearchTemplates(conn2, &ntstemplates2);

	for (i = 0; i < ntstemplates1; i++)
		logNoise("server1: %s.%s", tstemplates1[i].obj.schemaname,
				 tstemplates1[i].obj.objectname);

	for (i = 0; i < ntstemplates2; i++)
		logNoise("server2: %s.%s", tstemplates2[i].obj.schemaname,
				 tstemplates2[i].obj.objectname);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out tstemplates not presented in the other list.
	 */
	i = j = 0;
	while (i < ntstemplates1 || j < ntstemplates2)
	{
		/* End of tstemplates1 list. Print tstemplates2 list until its end. */
		if (i == ntstemplates1)
		{
			logDebug("text search template %s.%s: server2", tstemplates2[j].obj.schemaname,
					 tstemplates2[j].obj.objectname);

			dumpCreateTextSearchTemplate(fpre, &tstemplates2[j]);

			j++;
			qstat.tstemplateadded++;
		}
		/* End of tstemplates2 list. Print tstemplates1 list until its end. */
		else if (j == ntstemplates2)
		{
			logDebug("text search template %s.%s: server1", tstemplates1[i].obj.schemaname,
					 tstemplates1[i].obj.objectname);

			dumpDropTextSearchTemplate(fpost, &tstemplates1[i]);

			i++;
			qstat.tstemplateremoved++;
		}
		else if (compareRelations(&tstemplates1[i].obj, &tstemplates2[j].obj) == 0)
		{
			logDebug("text search template %s.%s: server1 server2",
					 tstemplates1[i].obj.schemaname,
					 tstemplates1[i].obj.objectname);

			dumpAlterTextSearchTemplate(fpre, &tstemplates1[i], &tstemplates2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&tstemplates1[i].obj, &tstemplates2[j].obj) < 0)
		{
			logDebug("text search template %s.%s: server1", tstemplates1[i].obj.schemaname,
					 tstemplates1[i].obj.objectname);

			dumpDropTextSearchTemplate(fpost, &tstemplates1[i]);

			i++;
			qstat.tstemplateremoved++;
		}
		else if (compareRelations(&tstemplates1[i].obj, &tstemplates2[j].obj) > 0)
		{
			logDebug("text search template %s.%s: server2", tstemplates2[j].obj.schemaname,
					 tstemplates2[j].obj.objectname);

			dumpCreateTextSearchTemplate(fpre, &tstemplates2[j]);

			j++;
			qstat.tstemplateadded++;
		}
	}

	freeTextSearchTemplates(tstemplates1, ntstemplates1);
	freeTextSearchTemplates(tstemplates2, ntstemplates2);
}

static void
quarrelTransforms()
{
	PQLTransform	*transforms1 = NULL;		/* target */
	PQLTransform	*transforms2 = NULL;		/* source */
	int				ntransforms1 = 0;		/* # of transforms */
	int				ntransforms2 = 0;
	int				i, j;

	transforms1 = getTransforms(conn1, &ntransforms1);
	transforms2 = getTransforms(conn2, &ntransforms2);

	for (i = 0; i < ntransforms1; i++)
		logNoise("server1: transform for %s.%s language %s",
				 transforms1[i].trftype.schemaname, transforms1[i].trftype.objectname,
				 transforms1[i].languagename);

	for (i = 0; i < ntransforms2; i++)
		logNoise("server2: transform for %s.%s language %s",
				 transforms2[i].trftype.schemaname, transforms2[i].trftype.objectname,
				 transforms2[i].languagename);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out transforms not presented in the other list.
	 */
	i = j = 0;
	while (i < ntransforms1 || j < ntransforms2)
	{
		/* End of transforms1 list. Print transforms2 list until its end. */
		if (i == ntransforms1)
		{
			logDebug("transform for %s.%s language %s: server2",
					 transforms2[i].trftype.schemaname, transforms2[i].trftype.objectname,
					 transforms2[i].languagename);

			dumpCreateTransform(fpre, &transforms2[j]);

			j++;
			qstat.transformadded++;
		}
		/* End of transforms2 list. Print transforms1 list until its end. */
		else if (j == ntransforms2)
		{
			logDebug("transform for %s.%s language %s: server1",
					 transforms1[i].trftype.schemaname, transforms1[i].trftype.objectname,
					 transforms1[i].languagename);

			dumpDropTransform(fpost, &transforms1[i]);

			i++;
			qstat.transformremoved++;
		}
		else if (compareNamesAndRelations(&transforms1[i].trftype,
										  &transforms2[j].trftype, transforms1[i].languagename,
										  transforms2[j].languagename) == 0)
		{
			logDebug("transform for %s.%s language %s: server1 server2",
					 transforms1[i].trftype.schemaname, transforms1[i].trftype.objectname,
					 transforms1[i].languagename);

			dumpAlterTransform(fpre, &transforms1[i], &transforms2[j]);

			i++;
			j++;
		}
		else if (compareNamesAndRelations(&transforms1[i].trftype,
										  &transforms2[j].trftype, transforms1[i].languagename,
										  transforms2[j].languagename) < 0)
		{
			logDebug("transform for %s.%s language %s: server1",
					 transforms1[i].trftype.schemaname, transforms1[i].trftype.objectname,
					 transforms1[i].languagename);

			dumpDropTransform(fpost, &transforms1[i]);

			i++;
			qstat.transformremoved++;
		}
		else if (compareNamesAndRelations(&transforms1[i].trftype,
										  &transforms2[j].trftype, transforms1[i].languagename,
										  transforms2[j].languagename) > 0)
		{
			logDebug("transform for %s.%s language %s: server2",
					 transforms2[i].trftype.schemaname, transforms2[i].trftype.objectname,
					 transforms2[i].languagename);

			dumpCreateTransform(fpre, &transforms2[j]);

			j++;
			qstat.transformadded++;
		}
	}

	freeTransforms(transforms1, ntransforms1);
	freeTransforms(transforms2, ntransforms2);
}

static void
quarrelTriggers()
{
	PQLTrigger	*triggers1 = NULL;	/* target */
	PQLTrigger	*triggers2 = NULL;	/* source */
	int			ntriggers1 = 0;		/* # of triggers */
	int			ntriggers2 = 0;
	int			i, j;

	triggers1 = getTriggers(conn1, &ntriggers1);
	triggers2 = getTriggers(conn2, &ntriggers2);

	for (i = 0; i < ntriggers1; i++)
		logNoise("server1: %s.%s", triggers1[i].table.schemaname,
				 triggers1[i].table.objectname);

	for (i = 0; i < ntriggers2; i++)
		logNoise("server2: %s.%s", triggers2[i].table.schemaname,
				 triggers2[i].table.objectname);

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

			dumpCreateTrigger(fpre, &triggers2[j]);

			j++;
			qstat.trgadded++;
		}
		/* End of triggers2 list. Print triggers1 list until its end. */
		else if (j == ntriggers2)
		{
			logDebug("trigger %s.%s: server1", triggers1[i].table.schemaname,
					 triggers1[i].table.objectname);

			dumpDropTrigger(fpost, &triggers1[i]);

			i++;
			qstat.trgremoved++;
		}
		else if (compareNamesAndRelations(&triggers1[i].table, &triggers2[j].table,
										  triggers1[i].trgname, triggers2[j].trgname) == 0)
		{
			logDebug("trigger %s.%s: server1 server2", triggers1[i].table.schemaname,
					 triggers1[i].table.objectname);

			dumpAlterTrigger(fpre, &triggers1[i], &triggers2[j]);

			i++;
			j++;
		}
		else if (compareNamesAndRelations(&triggers1[i].table, &triggers2[j].table,
										  triggers1[i].trgname, triggers2[j].trgname) < 0)
		{
			logDebug("trigger %s.%s: server1", triggers1[i].table.schemaname,
					 triggers1[i].table.objectname);

			dumpDropTrigger(fpost, &triggers1[i]);

			i++;
			qstat.trgremoved++;
		}
		else if (compareNamesAndRelations(&triggers1[i].table, &triggers2[j].table,
										  triggers1[i].trgname, triggers2[j].trgname) > 0)
		{
			logDebug("trigger %s.%s: server2", triggers2[j].table.schemaname,
					 triggers2[j].table.objectname);

			dumpCreateTrigger(fpre, &triggers2[j]);

			j++;
			qstat.trgadded++;
		}
	}

	freeTriggers(triggers1, ntriggers1);
	freeTriggers(triggers2, ntriggers2);
}

static void
quarrelBaseTypes()
{
	PQLBaseType	*types1 = NULL;	/* target */
	PQLBaseType	*types2 = NULL;	/* source */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getBaseTypes(conn1, &ntypes1);
	types2 = getBaseTypes(conn2, &ntypes2);

	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);

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

			if (options.securitylabels)
				getBaseTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateBaseType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropBaseType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			if (options.securitylabels)
			{
				getBaseTypeSecurityLabels(conn1, &types1[i]);
				getBaseTypeSecurityLabels(conn2, &types2[j]);
			}

			dumpAlterBaseType(fpre, &types1[i], &types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropBaseType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			if (options.securitylabels)
				getBaseTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateBaseType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
	}

	freeBaseTypes(types1, ntypes1);
	freeBaseTypes(types2, ntypes2);
}

static void
quarrelCompositeTypes()
{
	PQLCompositeType	*types1 = NULL;	/* target */
	PQLCompositeType	*types2 = NULL;	/* source */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getCompositeTypes(conn1, &ntypes1);
	types2 = getCompositeTypes(conn2, &ntypes2);

	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);

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

			if (options.securitylabels)
				getCompositeTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateCompositeType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropCompositeType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			if (options.securitylabels)
			{
				getCompositeTypeSecurityLabels(conn1, &types1[i]);
				getCompositeTypeSecurityLabels(conn2, &types2[j]);
			}

			dumpAlterCompositeType(fpre, &types1[i], &types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropCompositeType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			if (options.securitylabels)
				getCompositeTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateCompositeType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
	}

	freeCompositeTypes(types1, ntypes1);
	freeCompositeTypes(types2, ntypes2);
}

static void
quarrelEnumTypes()
{
	PQLEnumType	*types1 = NULL;	/* target */
	PQLEnumType	*types2 = NULL;	/* source */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getEnumTypes(conn1, &ntypes1);
	types2 = getEnumTypes(conn2, &ntypes2);

	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);

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

			if (options.securitylabels)
				getEnumTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateEnumType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropEnumType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			if (options.securitylabels)
			{
				getEnumTypeSecurityLabels(conn1, &types1[i]);
				getEnumTypeSecurityLabels(conn2, &types2[j]);
			}

			dumpAlterEnumType(fpre, &types1[i], &types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropEnumType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			if (options.securitylabels)
				getEnumTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateEnumType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
	}

	freeEnumTypes(types1, ntypes1);
	freeEnumTypes(types2, ntypes2);
}

static void
quarrelRangeTypes()
{
	PQLRangeType	*types1 = NULL;	/* target */
	PQLRangeType	*types2 = NULL;	/* source */
	int			ntypes1 = 0;		/* # of types */
	int			ntypes2 = 0;
	int			i, j;

	types1 = getRangeTypes(conn1, &ntypes1);
	types2 = getRangeTypes(conn2, &ntypes2);

	for (i = 0; i < ntypes1; i++)
		logNoise("server1: %s.%s", types1[i].obj.schemaname,
				 types1[i].obj.objectname);

	for (i = 0; i < ntypes2; i++)
		logNoise("server2: %s.%s", types2[i].obj.schemaname,
				 types2[i].obj.objectname);

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

			if (options.securitylabels)
				getRangeTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateRangeType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
		/* End of types2 list. Print types1 list until its end. */
		else if (j == ntypes2)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropRangeType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) == 0)
		{
			logDebug("type %s.%s: server1 server2", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			if (options.securitylabels)
			{
				getRangeTypeSecurityLabels(conn1, &types1[i]);
				getRangeTypeSecurityLabels(conn2, &types2[j]);
			}

			dumpAlterRangeType(fpre, &types1[i], &types2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) < 0)
		{
			logDebug("type %s.%s: server1", types1[i].obj.schemaname,
					 types1[i].obj.objectname);

			dumpDropRangeType(fpost, &types1[i]);

			i++;
			qstat.typeremoved++;
		}
		else if (compareRelations(&types1[i].obj, &types2[j].obj) > 0)
		{
			logDebug("type %s.%s: server2", types2[j].obj.schemaname,
					 types2[j].obj.objectname);

			if (options.securitylabels)
				getRangeTypeSecurityLabels(conn2, &types2[j]);

			dumpCreateRangeType(fpre, &types2[j]);

			j++;
			qstat.typeadded++;
		}
	}

	freeRangeTypes(types1, ntypes1);
	freeRangeTypes(types2, ntypes2);
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
quarrelUserMappings()
{
	PQLUserMapping		*usermappings1 = NULL;		/* target */
	PQLUserMapping		*usermappings2 = NULL;		/* source */
	int			nusermappings1 = 0;		/* # of user mappings */
	int			nusermappings2 = 0;
	int			i, j;

	usermappings1 = getUserMappings(conn1, &nusermappings1);
	usermappings2 = getUserMappings(conn2, &nusermappings2);

	for (i = 0; i < nusermappings1; i++)
		logNoise("server1: user(%s) server(%s)", usermappings1[i].user,
				 usermappings1[i].server);

	for (i = 0; i < nusermappings2; i++)
		logNoise("server2: user(%s) server(%s)", usermappings2[i].user,
				 usermappings2[i].server);

	/*
	 * We have two sorted lists. Let's figure out which elements are not in the
	 * other list.
	 * We have two sorted lists. The strategy is transverse both lists only once
	 * to figure out usermappings not presented in the other list.
	 */
	i = j = 0;
	while (i < nusermappings1 || j < nusermappings2)
	{
		/* End of usermappings1 list. Print usermappings2 list until its end. */
		if (i == nusermappings1)
		{
			logDebug("user mapping user(%s) server(%s): server2", usermappings2[j].user,
					 usermappings2[j].server);

			dumpCreateUserMapping(fpre, &usermappings2[j]);

			j++;
			qstat.usermappingadded++;
		}
		/* End of usermappings2 list. Print usermappings1 list until its end. */
		else if (j == nusermappings2)
		{
			logDebug("user mapping user(%s) server(%s): server1", usermappings1[i].user,
					 usermappings1[i].server);

			dumpDropUserMapping(fpost, &usermappings1[i]);

			i++;
			qstat.usermappingremoved++;
		}
		else if (compareUserMappings(&usermappings1[i], &usermappings2[j]) == 0)
		{
			logDebug("user mapping user(%s) server(%s): server1 server2",
					 usermappings1[i].user, usermappings1[i].server);

			dumpAlterUserMapping(fpre, &usermappings1[i], &usermappings2[j]);

			i++;
			j++;
		}
		else if (compareUserMappings(&usermappings1[i], &usermappings2[j]) < 0)
		{
			logDebug("user mapping user(%s) server(%s): server1", usermappings1[i].user,
					 usermappings1[i].server);

			dumpDropUserMapping(fpost, &usermappings1[i]);

			i++;
			qstat.usermappingremoved++;
		}
		else if (compareUserMappings(&usermappings1[i], &usermappings2[j]) > 0)
		{
			logDebug("user mapping user(%s) server(%s): server2", usermappings2[j].user,
					 usermappings2[j].server);

			dumpCreateUserMapping(fpre, &usermappings2[j]);

			j++;
			qstat.usermappingadded++;
		}
	}

	freeUserMappings(usermappings1, nusermappings1);
	freeUserMappings(usermappings2, nusermappings2);
}

static void
quarrelViews()
{
	PQLView	*views1 = NULL;	/* target */
	PQLView	*views2 = NULL;	/* source */
	int			nviews1 = 0;		/* # of views */
	int			nviews2 = 0;
	int			i, j;

	views1 = getViews(conn1, &nviews1);
	views2 = getViews(conn2, &nviews2);

	for (i = 0; i < nviews1; i++)
		logNoise("server1: %s.%s", views1[i].obj.schemaname,
				 views1[i].obj.objectname);

	for (i = 0; i < nviews2; i++)
		logNoise("server2: %s.%s", views2[i].obj.schemaname,
				 views2[i].obj.objectname);

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

			if (options.securitylabels)
				getViewSecurityLabels(conn2, &views2[j]);

			dumpCreateView(fpre, &views2[j]);

			j++;
			qstat.viewadded++;
		}
		/* End of views2 list. Print views1 list until its end. */
		else if (j == nviews2)
		{
			logDebug("view %s.%s: server1", views1[i].obj.schemaname,
					 views1[i].obj.objectname);

			dumpDropView(fpost, &views1[i]);

			i++;
			qstat.viewremoved++;
		}
		else if (compareRelations(&views1[i].obj, &views2[j].obj) == 0)
		{
			logDebug("view %s.%s: server1 server2", views1[i].obj.schemaname,
					 views1[i].obj.objectname);

			if (options.securitylabels)
			{
				getViewSecurityLabels(conn1, &views1[i]);
				getViewSecurityLabels(conn2, &views2[j]);
			}

			dumpAlterView(fpre, &views1[i], &views2[j]);

			i++;
			j++;
		}
		else if (compareRelations(&views1[i].obj, &views2[j].obj) < 0)
		{
			logDebug("view %s.%s: server1", views1[i].obj.schemaname,
					 views1[i].obj.objectname);

			dumpDropView(fpost, &views1[i]);

			i++;
			qstat.viewremoved++;
		}
		else if (compareRelations(&views1[i].obj, &views2[j].obj) > 0)
		{
			logDebug("view %s.%s: server2", views2[j].obj.schemaname,
					 views2[j].obj.objectname);

			if (options.securitylabels)
				getViewSecurityLabels(conn2, &views2[j]);

			dumpCreateView(fpre, &views2[j]);

			j++;
			qstat.viewadded++;
		}
	}

	freeViews(views1, nviews1);
	freeViews(views2, nviews2);
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
			logError("could not write to temporary file \"%s\": %s", prepath,
					 strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	/* EOF is expected to be reached. Check feof()? */
	if (ferror(pre) != 0)
	{
		logError("error while reading temporary file \"%s\": %s", prepath,
				 strerror(errno));
		exit(EXIT_FAILURE);
	}

	while (fgets(buf, sizeof(buf), post) != NULL)
	{
		if (fputs(buf, output) == EOF)
		{
			logError("could not write to temporary file \"%s\": %s", postpath,
					 strerror(errno));
			exit(EXIT_FAILURE);
		}
	}

	/* EOF is expected to be reached. Check feof()? */
	if (ferror(post) != 0)
	{
		logError("error while reading temporary file \"%s\": %s", postpath,
				 strerror(errno));
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
printSummary(void)
{
	fprintf(stderr, "%d table(s) added, %d table(s) removed\n", qstat.tableadded,
			qstat.tableremoved);
	fprintf(stderr, "%d sequence(s) added, %d sequence(s) removed\n",
			qstat.seqadded,
			qstat.seqremoved);
	fprintf(stderr, "%d index(es) added, %d index(es) removed\n", qstat.indexadded,
			qstat.indexremoved);
}

int main(int argc, char *argv[])
{
	static struct option long_options[] =
	{
		{"config", required_argument, NULL, 'c'},
		{"file", required_argument, NULL, 'f'},
		{"summary", no_argument, NULL, 's'},
		{"single-transaction", no_argument, NULL, 't'},
		{"verbose", no_argument, NULL, 'v'},
		/* the following options does not have an equivalent short letter */
		{"source-dbname", required_argument, NULL, 2},
		{"source-host", required_argument, NULL, 3},
		{"source-port", required_argument, NULL, 4},
		{"source-username", required_argument, NULL, 5},
		{"source-no-password", no_argument, NULL, 38},
		{"target-dbname", required_argument, NULL, 6},
		{"target-host", required_argument, NULL, 7},
		{"target-port", required_argument, NULL, 8},
		{"target-username", required_argument, NULL, 9},
		{"target-no-password", no_argument, NULL, 39},
		{"aggregate", required_argument, NULL, 10},
		{"cast", required_argument, NULL, 11},
		{"collation", required_argument, NULL, 12},
		{"comment", required_argument, NULL, 13},
		{"conversion", required_argument, NULL, 14},
		{"domain", required_argument, NULL, 15},
		{"event-trigger", required_argument, NULL, 16},
		{"extension", required_argument, NULL, 17},
		{"fdw", required_argument, NULL, 18},
		{"foreign-table", required_argument, NULL, 43},
		{"function", required_argument, NULL, 19},
		{"index", required_argument, NULL, 20},
		{"language", required_argument, NULL, 21},
		{"materialized-view", required_argument, NULL, 22},
		{"operator", required_argument, NULL, 23},
		{"owner", required_argument, NULL, 24},
		{"privileges", required_argument, NULL, 25},
		{"procedure", required_argument, NULL, 42},
		{"publication", required_argument, NULL, 40},
		{"rule", required_argument, NULL, 26},
		{"schema", required_argument, NULL, 27},
		{"security-labels", required_argument, NULL, 28},
		{"sequence", required_argument, NULL, 29},
		{"statistics", required_argument, NULL, 30},
		{"subscription", required_argument, NULL, 41},
		{"table", required_argument, NULL, 31},
		{"text-search", required_argument, NULL, 32},
		{"transform", required_argument, NULL, 44},
		{"trigger", required_argument, NULL, 33},
		{"type", required_argument, NULL, 34},
		{"view", required_argument, NULL, 35},
		{"ignore-version", no_argument, NULL, 36},
		{"temp-directory", required_argument, NULL, 37},
		{"include-schema", required_argument, NULL, 45},
		{"exclude-schema", required_argument, NULL, 46},
		{"table-partition", required_argument, NULL, 47},
		{NULL, 0, NULL, 0}
	};

	int			optindex;
	int			c;

	char		*configfile = NULL;

	bool		output_given = false;
	bool		tmpdir_given = false;
	bool		source_prompt_given = false;
	bool		target_prompt_given = false;
	bool		include_schema_given = false;
	bool		exclude_schema_given = false;

	/* general and connection options */
	QuarrelOptions	opts;

	/* command-line options */
	QuarrelGeneralOptions	gopts_given;	/* command-line informed? */
	QuarrelGeneralOptions	gopts;
	QuarrelDatabaseOptions	sopts;
	QuarrelDatabaseOptions	topts;

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0)
		{
			help();
			exit(EXIT_SUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0)
		{
			printf(PGQ_NAME " " PGQ_VERSION "\n");
			exit(EXIT_SUCCESS);
		}
	}

	/* no command-line options have been informed yet */
	memset(&gopts_given, 0, sizeof(QuarrelGeneralOptions));
	memset(&gopts, 0, sizeof(QuarrelGeneralOptions));
	memset(&sopts, 0, sizeof(QuarrelDatabaseOptions));
	memset(&topts, 0, sizeof(QuarrelDatabaseOptions));

	/* source or target? */
	opts.source.istarget = false;
	opts.target.istarget = true;

	/* process command-line options */
	while ((c = getopt_long(argc, argv, "c:f:stv", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'c':
				configfile = strdup(optarg);
				break;
			case 'f':
				gopts.output = strdup(optarg);
				output_given = true;
				break;
			case 's':
				gopts.summary = true;
				gopts_given.summary = true;
				break;
			case 't':
				gopts.singletxn = true;
				gopts_given.singletxn = true;
				break;
			case 'v':
				if (loglevel == PGQ_ERROR)
					loglevel = PGQ_WARNING;
				else if (loglevel == PGQ_WARNING)
					loglevel = PGQ_DEBUG;
				else if (loglevel == PGQ_DEBUG)
					loglevel = PGQ_NOISE;
				break;
			case 0:
				/* this cover the long options */
				break;
			case 2:
				sopts.dbname = strdup(optarg);		/* source.dbname */
				break;
			case 3:
				sopts.host = strdup(optarg);		/* source.host */
				break;
			case 4:
				sopts.port = strdup(optarg);		/* source.port */
				break;
			case 5:
				sopts.username = strdup(optarg);	/* source.username */
				break;
			case 6:
				topts.dbname = strdup(optarg);		/* target.dbname */
				break;
			case 7:
				topts.host = strdup(optarg);		/* target.host */
				break;
			case 8:
				topts.port = strdup(optarg);		/* target.port */
				break;
			case 9:
				topts.username = strdup(optarg);	/* target.username */
				break;
			case 10:
				gopts.aggregate = parseBoolean("aggregate", optarg);
				gopts_given.aggregate = true;
				break;
			case 11:
				gopts.cast = parseBoolean("cast", optarg);
				gopts_given.cast = true;
				break;
			case 12:
				gopts.collation = parseBoolean("collation", optarg);
				gopts_given.collation = true;
				break;
			case 13:
				gopts.comment = parseBoolean("comment", optarg);
				gopts_given.comment = true;
				break;
			case 14:
				gopts.conversion = parseBoolean("conversion", optarg);
				gopts_given.conversion = true;
				break;
			case 15:
				gopts.domain = parseBoolean("domain", optarg);
				gopts_given.domain = true;
				break;
			case 16:
				gopts.eventtrigger = parseBoolean("event-trigger", optarg);
				gopts_given.eventtrigger = true;
				break;
			case 17:
				gopts.extension = parseBoolean("extension", optarg);
				gopts_given.extension = true;
				break;
			case 18:
				gopts.fdw = parseBoolean("fdw", optarg);
				gopts_given.fdw = true;
				break;
			case 19:
				gopts.function = parseBoolean("function", optarg);
				gopts_given.function = true;
				break;
			case 20:
				gopts.index = parseBoolean("index", optarg);
				gopts_given.index = true;
				break;
			case 21:
				gopts.language = parseBoolean("language", optarg);
				gopts_given.language = true;
				break;
			case 22:
				gopts.matview = parseBoolean("materialized-view", optarg);
				gopts_given.matview = true;
				break;
			case 23:
				gopts.operator = parseBoolean("operator", optarg);
				gopts_given.operator = true;
				break;
			case 24:
				gopts.owner = parseBoolean("owner", optarg);
				gopts_given.owner = true;
				break;
			case 25:
				gopts.privileges = parseBoolean("privileges", optarg);
				gopts_given.privileges = true;
				break;
			case 26:
				gopts.rule = parseBoolean("rule", optarg);
				gopts_given.rule = true;
				break;
			case 27:
				gopts.schema = parseBoolean("schema", optarg);
				gopts_given.schema = true;
				break;
			case 28:
				gopts.securitylabels = parseBoolean("security-labels", optarg);
				gopts_given.securitylabels = true;
				break;
			case 29:
				gopts.sequence = parseBoolean("sequence", optarg);
				gopts_given.sequence = true;
				break;
			case 30:
				gopts.statistics = parseBoolean("statistics", optarg);
				gopts_given.statistics = true;
				break;
			case 31:
				gopts.table = parseBoolean("table", optarg);
				gopts_given.table = true;
				break;
			case 47:
				gopts.tablepartition = parseBoolean("table-partition", optarg);
				gopts_given.tablepartition = true;
				break;				
			case 32:
				gopts.textsearch = parseBoolean("text-search", optarg);
				gopts_given.textsearch = true;
				break;
			case 33:
				gopts.trigger = parseBoolean("trigger", optarg);
				gopts_given.trigger = true;
				break;
			case 34:
				gopts.type = parseBoolean("type", optarg);
				gopts_given.type = true;
				break;
			case 35:
				gopts.view = parseBoolean("view", optarg);
				gopts_given.view = true;
				break;
			case 36:
				gopts.ignoreversion = true;
				gopts_given.ignoreversion = true;
				break;
			case 37:
				gopts.tmpdir = strdup(optarg);
				tmpdir_given = true;
				break;
			case 38:
				sopts.promptpassword = false;
				source_prompt_given = true;
				break;
			case 39:
				topts.promptpassword = false;
				target_prompt_given = true;
				break;
			case 40:
				gopts.publication = parseBoolean("publication", optarg);
				gopts_given.publication = true;
				break;
			case 41:
				gopts.subscription = parseBoolean("subscription", optarg);
				gopts_given.subscription = true;
				break;
			case 42:
				gopts.procedure = parseBoolean("procedure", optarg);
				gopts_given.procedure = true;
				break;
			case 43:
				gopts.foreigntable = parseBoolean("foreign-table", optarg);
				gopts_given.foreigntable = true;
				break;
			case 44:
				gopts.transform = parseBoolean("transform", optarg);
				gopts_given.transform = true;
				break;
			case 45:
				gopts.include_schema = strdup(optarg);
				include_schema_given = true;
				break;
			case 46:
				gopts.exclude_schema = strdup(optarg);
				exclude_schema_given = true;
				break;
			default:
				fprintf(stderr, "Try \"%s --help\" for more information.\n", PGQ_NAME);
				exit(EXIT_FAILURE);
		}
	}

	/* read configuration file */
	loadConfig(configfile, &opts);

	/* config filename was not used anymore; free it */
	if (configfile)
		free(configfile);

	/* expose only general options */
	options = opts.general;

	/*
	 * command-line options take precedence over config options
	 */
	if (output_given)
		options.output = gopts.output;
	if (tmpdir_given)
		options.tmpdir = gopts.tmpdir;

	if (options.verbose && loglevel == PGQ_ERROR)
		loglevel = PGQ_DEBUG;
	if (gopts_given.ignoreversion)
		options.ignoreversion = gopts.ignoreversion;
	if (gopts_given.summary)
		options.summary = gopts.summary;
	if (gopts_given.singletxn)
		options.singletxn = gopts.singletxn;

	if (gopts_given.aggregate)
		options.aggregate = gopts.aggregate;
	if (gopts_given.cast)
		options.cast = gopts.cast;
	if (gopts_given.collation)
		options.collation = gopts.collation;
	if (gopts_given.comment)
		options.comment = gopts.comment;
	if (gopts_given.conversion)
		options.conversion = gopts.conversion;
	if (gopts_given.domain)
		options.domain = gopts.domain;
	if (gopts_given.eventtrigger)
		options.eventtrigger = gopts.eventtrigger;
	if (gopts_given.extension)
		options.extension = gopts.extension;
	if (gopts_given.fdw)
		options.fdw = gopts.fdw;
	if (gopts_given.foreigntable)
		options.foreigntable = gopts.foreigntable;
	if (gopts_given.function)
		options.function = gopts.function;
	if (gopts_given.index)
		options.index = gopts.index;
	if (gopts_given.language)
		options.language = gopts.language;
	if (gopts_given.matview)
		options.matview = gopts.matview;
	if (gopts_given.operator)
		options.operator = gopts.operator;
	if (gopts_given.owner)
		options.owner = gopts.owner;
	if (gopts_given.privileges)
		options.privileges = gopts.privileges;
	if (gopts_given.procedure)
		options.procedure = gopts.procedure;
	if (gopts_given.rule)
		options.rule = gopts.rule;
	if (gopts_given.publication)
		options.publication = gopts.publication;
	if (gopts_given.schema)
		options.schema = gopts.schema;
	if (gopts_given.securitylabels)
		options.securitylabels = gopts.securitylabels;
	if (gopts_given.sequence)
		options.sequence = gopts.sequence;
	if (gopts_given.statistics)
		options.statistics = gopts.statistics;
	if (gopts_given.subscription)
		options.subscription = gopts.subscription;
	if (gopts_given.table)
		options.table = gopts.table;
	if (gopts_given.tablepartition)
		options.tablepartition = gopts.tablepartition;		
	if (gopts_given.textsearch)
		options.textsearch = gopts.textsearch;
	if (gopts_given.transform)
		options.transform = gopts.transform;
	if (gopts_given.trigger)
		options.trigger = gopts.trigger;
	if (gopts_given.type)
		options.type = gopts.type;
	if (gopts_given.view)
		options.view = gopts.view;

	if (include_schema_given)
		options.include_schema = gopts.include_schema;
	if (exclude_schema_given)
		options.exclude_schema = gopts.exclude_schema;

	if (sopts.dbname)
		opts.source.dbname = sopts.dbname;
	if (sopts.host)
		opts.source.host = sopts.host;
	if (sopts.port)
		opts.source.port = sopts.port;
	if (sopts.username)
		opts.source.username = sopts.username;
	if (source_prompt_given)
		opts.source.promptpassword = sopts.promptpassword;

	if (topts.dbname)
		opts.target.dbname = topts.dbname;
	if (topts.host)
		opts.target.host = topts.host;
	if (topts.port)
		opts.target.port = topts.port;
	if (topts.username)
		opts.target.username = topts.username;
	if (target_prompt_given)
		opts.target.promptpassword = topts.promptpassword;

	/* connecting to server1 ... */
	conn1 = connectDatabase(opts.target);
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
	conn2 = connectDatabase(opts.source);
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
	if (!options.ignoreversion &&
			((compareMajorVersion(pgversion1, PG_VERSION_NUM) > 0) ||
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
	if (options.output != NULL && strcmp(options.output, "-") != 0)
	{
		fout = fopen(options.output, "w");
		if (fout == NULL)
		{
			logError("could not open output file \"%s\"", options.output);
			exit(EXIT_FAILURE);
		}
	}
	else
		fout = stdout;

	/* temporary files are used to put commands in the right dependency order */
	snprintf(prepath, PGQMAXPATH, "%s/quarrel.%d.pre", options.tmpdir, getpid());
	snprintf(postpath, PGQMAXPATH, "%s/quarrel.%d.post", options.tmpdir, getpid());
	fpre = openTempFile(prepath);
	fpost = openTempFile(postpath);

	/*
	 * Let's start the party ...
	 */

	/*
	 * Filter by schema. Multiple objects can use it, hence, build a unique string.
	 */
	if (options.include_schema != NULL)
	{
		include_schema_str = psprintf(" AND n.nspname ~ '%s'", options.include_schema);

		logNoise("filter include schema: %s", include_schema_str);
	}
	else
	{
		include_schema_str = "";
	}
	if (options.exclude_schema != NULL)
	{
		exclude_schema_str = psprintf(" AND n.nspname !~ '%s'", options.exclude_schema);

		logNoise("filter exclude schema: %s", exclude_schema_str);
	}
	else
	{
		exclude_schema_str = "";
	}

	/*
	 * We cannot start sending everything to stdout here because we will have
	 * problems with the dependencies. Generally, CREATE commands will be output
	 * at the beginning (in a pre-defined order) and DROP commands at the end
	 * (in a reverse order -- to solve dependency problems).
	 * Only selected objects will be compared.
	 */

	if (options.fdw)
	{
		quarrelForeignDataWrappers();
		quarrelForeignServers();
		quarrelUserMappings();
	}

	if (options.language)
		quarrelLanguages();
	if (options.schema)
		quarrelSchemas();
	if (options.extension)
		quarrelExtensions();
	if (options.accessmethod)
		quarrelAccessMethods();

	if (options.cast)
		quarrelCasts();
	if (options.collation)
		quarrelCollations();
	if (options.conversion)
		quarrelConversions();
	if (options.domain)
		quarrelDomains();
	if (options.type)
		quarrelTypes();
	if (options.operator)
	{
		quarrelOperators();
		quarrelOperatorFamilies();
		quarrelOperatorClasses();
	}
	if (options.sequence)
		quarrelSequences();
	if (options.table)
		quarrelTables();
	if (options.index)
		quarrelIndexes();
	if (options.function)
		quarrelFunctions();
	if (options.procedure)
		quarrelProcedures();
	if (options.foreigntable)
		quarrelForeignTables();
	if (options.aggregate)
		quarrelAggregates();
	if (options.view)
		quarrelViews();
	if (options.matview)
		quarrelMaterializedViews();
	if (options.trigger)
		quarrelTriggers();
	if (options.rule)
		quarrelRules();
	if (options.publication)
		quarrelPublications();
	if (options.subscription)
		quarrelSubscriptions();
	if (options.policy)
		quarrelPolicies();
	if (options.eventtrigger)
		quarrelEventTriggers();
	if (options.textsearch)
	{
		quarrelTextSearchParsers();
		quarrelTextSearchTemplates();
		quarrelTextSearchDicts();
		quarrelTextSearchConfigs();
	}
	if (options.transform)
		quarrelTransforms();
	if (options.statistics)
		quarrelStatistics();

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

	/* execute as a single transaction */
	if (options.singletxn && (!isEmptyFile(prepath) || !isEmptyFile(postpath)))
		fprintf(fout, "\n\nBEGIN;");

	/* dump the quarrel in the right order */
	mergeTempFiles(fpre, fpost, fout);

	/* close single transaction */
	if (options.singletxn && (!isEmptyFile(prepath) || !isEmptyFile(postpath)))
		fprintf(fout, "\n\nCOMMIT;");

	/* close and remove temporary files */
	closeTempFile(fpre, prepath);
	closeTempFile(fpost, postpath);

	/* closing connections ... */
	PQfinish(conn1);
	PQfinish(conn2);

	logDebug("server1 connection is closed");
	logDebug("server2 connection is closed");

	if (options.summary)
		printSummary();

	/* flush and close the output file */
	fflush(fout);
	if (options.output != NULL && strcmp(options.output, "-") != 0)
		fclose(fout);
	else
		fprintf(fout, "\n");	/* new line for stdout */

	return 0;
}
