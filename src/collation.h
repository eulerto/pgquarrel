/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
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
	char			*provider;
	char			*comment;
	char			*owner;
} PQLCollation;

PQLCollation *getCollations(PGconn *c, int *n);
void getCollationConstraints(PGconn *c, PQLCollation *d);

void dumpDropCollation(FILE *output, PQLCollation *c);
void dumpCreateCollation(FILE *output, PQLCollation *c);
void dumpAlterCollation(FILE *output, PQLCollation *a, PQLCollation *b);

void freeCollations(PQLCollation *c, int n);

#endif	/* COLLATION_H */
