/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef TEXTSEARCH_H
#define TEXTSEARCH_H

#include "common.h"

typedef struct PQLTextSearchConfig
{
	PQLObject	obj;
	char		*prs;		/* parser */
	char		*dict;		/* dictionary */
	char		*tokentype;
	char		*comment;
	char		*owner;
} PQLTextSearchConfig;

typedef struct PQLTextSearchDict
{
	PQLObject	obj;
	char		*tmpl;		/* template */
	char		*options;
	char		*comment;
	char		*owner;
} PQLTextSearchDict;

typedef struct PQLTextSearchParser
{
	PQLObject	obj;
	char		*startfunc;
	char		*tokenfunc;
	char		*endfunc;
	char		*lextypesfunc;
	char		*headlinefunc;
	char		*comment;
} PQLTextSearchParser;

typedef struct PQLTextSearchTemplate
{
	PQLObject	obj;
	char		*initfunc;
	char		*lexizefunc;
	char		*comment;
} PQLTextSearchTemplate;

PQLTextSearchConfig *getTextSearchConfigs(PGconn *c, int *n);
PQLTextSearchDict *getTextSearchDicts(PGconn *c, int *n);
PQLTextSearchParser *getTextSearchParsers(PGconn *c, int *n);
PQLTextSearchTemplate *getTextSearchTemplates(PGconn *c, int *n);

void dumpDropTextSearchConfig(FILE *output, PQLTextSearchConfig *t);
void dumpDropTextSearchDict(FILE *output, PQLTextSearchDict *t);
void dumpDropTextSearchParser(FILE *output, PQLTextSearchParser *t);
void dumpDropTextSearchTemplate(FILE *output, PQLTextSearchTemplate *t);

void dumpCreateTextSearchConfig(FILE *output, PQLTextSearchConfig *c);
void dumpCreateTextSearchDict(FILE *output, PQLTextSearchDict *d);
void dumpCreateTextSearchParser(FILE *output, PQLTextSearchParser *p);
void dumpCreateTextSearchTemplate(FILE *output, PQLTextSearchTemplate *t);

void dumpAlterTextSearchConfig(FILE *output, PQLTextSearchConfig *a,
							   PQLTextSearchConfig *b);
void dumpAlterTextSearchDict(FILE *output, PQLTextSearchDict *a,
							 PQLTextSearchDict *b);
void dumpAlterTextSearchParser(FILE *output, PQLTextSearchParser *a,
							   PQLTextSearchParser *b);
void dumpAlterTextSearchTemplate(FILE *output, PQLTextSearchTemplate *a,
								 PQLTextSearchTemplate *b);

void freeTextSearchConfigs(PQLTextSearchConfig *t, int n);
void freeTextSearchDicts(PQLTextSearchDict *t, int n);
void freeTextSearchParsers(PQLTextSearchParser *t, int n);
void freeTextSearchTemplates(PQLTextSearchTemplate *t, int n);

#endif
