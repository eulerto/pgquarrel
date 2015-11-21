#ifndef LANGUAGE_H
#define LANGUAGE_H

#include "common.h"
#include "privileges.h"

typedef struct PQLLanguage
{
	char	*languagename;
	bool	pltemplate;
	bool	trusted;
	char	*callhandler;
	char	*inlinehandler;
	char	*validator;
	char	*comment;
	char	*owner;
	char	*acl;
} PQLLanguage;

PQLLanguage *getLanguages(PGconn *c, int *n);
void dumpDropLanguage(FILE *output, PQLLanguage s);
void dumpCreateLanguage(FILE *output, PQLLanguage s);
void dumpAlterLanguage(FILE *output, PQLLanguage a, PQLLanguage b);

#endif	/* LANGUAGE_H */
