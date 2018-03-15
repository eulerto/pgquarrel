/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef LANGUAGE_H
#define LANGUAGE_H

#include "common.h"
#include "privileges.h"

typedef struct PQLLanguage
{
	Oid		oid;
	char	*languagename;
	bool	pltemplate;
	bool	trusted;
	char	*callhandler;
	char	*inlinehandler;
	char	*validator;
	char	*comment;
	char	*owner;
	char	*acl;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLLanguage;

PQLLanguage *getLanguages(PGconn *c, int *n);
void getLanguageSecurityLabels(PGconn *c, PQLLanguage *l);

void dumpDropLanguage(FILE *output, PQLLanguage *l);
void dumpCreateLanguage(FILE *output, PQLLanguage *l);
void dumpAlterLanguage(FILE *output, PQLLanguage *a, PQLLanguage *b);

void freeLanguages(PQLLanguage *l, int n);

#endif	/* LANGUAGE_H */
