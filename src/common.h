/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <libpq-fe.h>
#include <c.h>			/* bool, true, false */
#include <postgres_fe.h>

#if defined(_WINDOWS)
#define	strcasecmp	_stricmp
#define	snprintf	_snprintf
#endif

/*
 * Same as FirstNormalObjectId in access/transam.h. This value does not change
 * since a long time ago (2005). It is safe to use this value as a cutting
 * point for user oids x system oids.
 */
#define	PGQ_FIRST_USER_OID		16384

#define	PGQMAXPATH			300

#define logFatal(...) do { \
	logGeneric(PGQ_FATAL, __VA_ARGS__); \
	} while (0)

#define logError(...) do { \
	logGeneric(PGQ_ERROR, __VA_ARGS__); \
	} while (0)

#define logWarning(...) do { \
	logGeneric(PGQ_WARNING, __VA_ARGS__); \
	} while (0)

#define logDebug(...) do { \
	logGeneric(PGQ_DEBUG, __VA_ARGS__); \
	} while (0)

#define logNoise(...) do { \
	logGeneric(PGQ_NOISE, __VA_ARGS__); \
	} while (0)

typedef struct QuarrelGeneralOptions
{
	char			*output;
	char			*tmpdir;
	bool			ignoreversion;
	bool			verbose;
	bool			summary;
	bool			comment;
	bool			owner;
	bool			privileges;
	bool			securitylabels;
	bool			singletxn;

	/* select objects */
	bool			accessmethod;
	bool			aggregate;
	bool			cast;
	bool			collation;
	bool			conversion;
	bool			domain;
	bool			eventtrigger;
	bool			extension;
	bool			fdw;
	bool			foreigntable;
	bool			function;
	bool			index;
	bool			language;
	bool			matview;
	bool			operator;
	bool			policy;
	bool			procedure;
	bool			publication;
	bool			rule;
	bool			schema;
	bool			sequence;
	bool			statistics;
	bool			subscription;
	bool			table;
	bool			textsearch;
	bool			transform;
	bool			trigger;
	bool			type;
	bool			view;

	/* filter options */
	char			*include_schema;
	char			*exclude_schema;
} QuarrelGeneralOptions;

typedef struct QuarrelDatabaseOptions
{
	char			*host;
	char			*port;
	char			*username;
	char			*password;
	char			*dbname;
	bool			istarget;
	bool			promptpassword;
} QuarrelDatabaseOptions;

typedef struct QuarrelOptions
{
	QuarrelGeneralOptions	general;
	QuarrelDatabaseOptions	source;
	QuarrelDatabaseOptions	target;
} QuarrelOptions;

typedef struct PQLSecLabel
{
	char	*provider;
	char	*label;
} PQLSecLabel;

typedef struct PQLObject
{
	Oid		oid;
	char	*schemaname;
	char	*objectname;
} PQLObject;

typedef struct PQLAttribute
{
	int			attnum;
	char		*attname;
	bool		attnotnull;
	char		*atttypname;
	char		*attdefexpr;
	char		*attcollation;
	int			attstattarget;
	char		*attstorage;
	bool		defstorage;
	char        attidentity;
	char		*attoptions;
	char		*attfdwoptions;
	char		*comment;
	char		*acl;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLAttribute;

typedef struct PQLConstraint
{
	char	*conname;
	char	*condef;
	bool	convalidated;
	char	*comment;
} PQLConstraint;

enum PQLLogLevel
{
	PGQ_FATAL = 0,
	PGQ_ERROR = 1,
	PGQ_WARNING = 2,
	PGQ_DEBUG = 3,
	PGQ_NOISE = 4
};

enum PQLSetOperation
{
	PGQ_INTERSECT = 0,
	PGQ_SETDIFFERENCE = 1
};

extern enum PQLLogLevel loglevel;
extern int pgversion1;
extern int pgversion2;

extern QuarrelGeneralOptions options;

extern char *include_schema_str;
extern char *exclude_schema_str;

typedef struct stringListCell
{
	struct stringListCell	*next;
	char					*value;
} stringListCell;

typedef struct stringList
{
	stringListCell	*head;
	stringListCell	*tail;
} stringList;


int compareRelations(PQLObject *a, PQLObject *b);
int compareNamesAndRelations(PQLObject *a, PQLObject *b, char *aname,
							 char *bname);
char *formatObjectIdentifier(char *s);
void logGeneric(enum PQLLogLevel level, const char *fmt, ...);

stringList *buildStringList(char *options);
stringList *setOperationOptions(char *a, char *b, int kind, bool withvalue,
								bool changed);
char *printOptions(stringList *sl);

void appendStringList(stringList *sl, const char *s);
void appendAllStringList(stringList *sl, char *s, const char *d);
void printStringList(FILE *fd, stringList sl);
#ifdef _NOT_USED
bool searchStringList(stringList *sl, const char *s);
#endif
void freeStringList(stringList *sl);

#endif	/* COMMON_H */
