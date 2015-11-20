#ifndef FUNCTION_H
#define FUNCTION_H

#include "common.h"

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
} PQLFunction;

PQLFunction *getFunctions(PGconn *c, int *n);
void getFunctionAttributes(PGconn *c, PQLFunction *f);
int compareFunctions(PQLFunction a, PQLFunction b);

void dumpDropFunction(FILE *output, PQLFunction f);
void dumpCreateFunction(FILE *output, PQLFunction f, bool orreplace);
void dumpAlterFunction(FILE *output, PQLFunction a, PQLFunction b);

#endif	/* FUNCTION_H */
