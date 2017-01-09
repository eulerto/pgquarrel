/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2017, Euler Taveira
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

/*
 * Same as FirstNormalObjectId in access/transam.h. This value does not change
 * since a long time ago (2005). It is safe to use this value as a cutting
 * point for user oids x system oids.
 */
#define	PGQ_FIRST_USER_OID		16384

#define	PGQMAXPATH			300
#define	PGQQRYLEN			300

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

typedef struct QuarrelOptions
{
	/* General */
	char			*output;
	char			*tmpdir;
	bool			verbose;
	bool			statistics;
	bool			comment;
	bool			owner;
	bool			privileges;
	bool			securitylabels;

	/* From */
	char			*fhost;
	char			*fport;
	char			*fusername;
	char			*fpassword;
	char			*fdbname;

	/* To */
	char			*thost;
	char			*tport;
	char			*tusername;
	char			*tpassword;
	char			*tdbname;
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
	char		*attoptions;
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

extern QuarrelOptions options;

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
stringList *setOperationOptions(char *a, char *b, int kind, bool withvalue, bool changed);
char *printOptions(stringList *sl);

void appendStringList(stringList *sl, const char *s);
void appendAllStringList(stringList *sl, char *s, const char *d);
void printStringList(FILE *fd, stringList sl);
#ifdef _NOT_USED
bool searchStringList(stringList *sl, const char *s);
#endif
void freeStringList(stringList *sl);

#endif	/* COMMON_H */
