#ifndef COLLATION_H
#define COLLATION_H

#include "common.h"
#include "privileges.h"

typedef struct PQLCollation
{
	PQLObject		obj;

	char			*encoding;
	char			*collate;
	char			*ctype;
	char			*comment;
	char			*owner;
} PQLCollation;

PQLCollation *getCollations(PGconn *c, int *n);
void getCollationConstraints(PGconn *c, PQLCollation *d);

void dumpDropCollation(FILE *output, PQLCollation d);
void dumpCreateCollation(FILE *output, PQLCollation d);
void dumpAlterCollation(FILE *output, PQLCollation a, PQLCollation b);

void freeCollations(PQLCollation *c, int n);

#endif	/* COLLATION_H */
