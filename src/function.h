/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
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
	char			*iarguments;	/* without DEFAULTs */
	char			*returntype;
	char			*language;
	char			kind;
	char			funcvolatile;
	bool			isstrict;
	bool			secdefiner;
	bool			leakproof;
	char			parallel;
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
PQLFunction *getProcedures(PGconn *c, int *n);
PQLFunction *getProcFunctions(PGconn *c, int *n, char t);
int compareFunctions(PQLFunction *a, PQLFunction *b);
void getFunctionSecurityLabels(PGconn *c, PQLFunction *f);
void getProcedureSecurityLabels(PGconn *c, PQLFunction *f);
void getProcFunctionSecurityLabels(PGconn *c, PQLFunction *f, char t);

void dumpDropFunction(FILE *output, PQLFunction *f);
void dumpCreateFunction(FILE *output, PQLFunction *f, bool orreplace);
void dumpAlterFunction(FILE *output, PQLFunction *a, PQLFunction *b);

void dumpDropProcedure(FILE *output, PQLFunction *f);
void dumpCreateProcedure(FILE *output, PQLFunction *f, bool orreplace);
void dumpAlterProcedure(FILE *output, PQLFunction *a, PQLFunction *b);

void dumpDropProcFunction(FILE *output, PQLFunction *f, char t);
void dumpCreateProcFunction(FILE *output, PQLFunction *f, bool orreplace,
							char t);
void dumpAlterProcFunction(FILE *output, PQLFunction *a, PQLFunction *b,
						   char t);

void freeFunctions(PQLFunction *f, int n);

#endif	/* FUNCTION_H */
