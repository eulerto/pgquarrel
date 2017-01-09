/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2017, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef FUNCTION_H
#define FUNCTION_H

#include "common.h"
#include "privileges.h"

typedef struct PQLFunction
{
	PQLObject		obj;

	char			*arguments;
	char			*returntype;
	char			*language;
	bool			iswindow;
	char			funcvolatile;
	bool			isstrict;
	bool			secdefiner;
	bool			leakproof;
	char			*cost;
	char			*rows;
	char			*configparams;
	char			*body;
	char			*comment;
	char			*owner;
	char			*acl;

	/* security labels */
	PQLSecLabel		*seclabels;
	int				nseclabels;
} PQLFunction;

PQLFunction *getFunctions(PGconn *c, int *n);
int compareFunctions(PQLFunction *a, PQLFunction *b);
void getFunctionSecurityLabels(PGconn *c, PQLFunction *f);

void dumpDropFunction(FILE *output, PQLFunction *f);
void dumpCreateFunction(FILE *output, PQLFunction *f, bool orreplace);
void dumpAlterFunction(FILE *output, PQLFunction *a, PQLFunction *b);

void freeFunctions(PQLFunction *f, int n);

#endif	/* FUNCTION_H */
