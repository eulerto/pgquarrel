/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef TRANSFORM_H
#define TRANSFORM_H

#include "common.h"

typedef struct PQLTransform
{
	PQLObject	trftype;
	char		*languagename;
	PQLObject	fromsql;
	char		*fromsqlargs;
	PQLObject	tosql;
	char		*tosqlargs;
	char		*comment;
} PQLTransform;

PQLTransform *getTransforms(PGconn *c, int *n);

void dumpDropTransform(FILE *output, PQLTransform *t);
void dumpCreateTransform(FILE *output, PQLTransform *t);
void dumpAlterTransform(FILE *output, PQLTransform *a, PQLTransform *b);

void freeTransforms(PQLTransform *t, int n);

#endif	/* TRANSFORM_H */
