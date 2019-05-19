/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef ACCESSMETHOD_H
#define ACCESSMETHOD_H

#include "common.h"

enum PQLAccessMethodType
{
	PGQ_AMTYPE_INDEX = 'i',
	PGQ_AMTYPE_TABLE = 't'
};

typedef struct PQLAccessMethod
{
	Oid			oid;
	char		*amname;
	char		amtype;
	PQLObject	handler;

	char		*comment;
} PQLAccessMethod;

PQLAccessMethod *getAccessMethods(PGconn *c, int *n);

void dumpDropAccessMethod(FILE *output, PQLAccessMethod *a);
void dumpCreateAccessMethod(FILE *output, PQLAccessMethod *a);
void dumpAlterAccessMethod(FILE *output, PQLAccessMethod *a, PQLAccessMethod *b);

void freeAccessMethods(PQLAccessMethod *a, int n);

#endif	/* ACCESSMETHOD_H */

