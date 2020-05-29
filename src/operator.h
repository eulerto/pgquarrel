/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef OPERATOR_H
#define	OPERATOR_H

#include "common.h"

typedef struct PQLOpOperators
{
	int			strategy;
	char		*oprname;
	PQLObject	sortfamily;
} PQLOpOperators;

typedef struct PQLOpFunctions
{
	int		support;
	char	*funcname;
} PQLOpFunctions;

typedef struct PQLOpAndFunc
{
	PQLOpOperators	*operators;
	int				noperators;
	PQLOpFunctions	*functions;
	int				nfunctions;
} PQLOpAndFunc;

typedef struct PQLOperator
{
	PQLObject		obj;
	char			*procedure;
	char			*lefttype;
	char			*righttype;
	char			*commutator;
	char			*negator;
	char			*restriction;	/* restriction selectivity estimator */
	char			*join;			/* join selectivity estimator */
	bool			canhash;
	bool			canmerge;

	char			*comment;
	char			*owner;
} PQLOperator;

typedef struct PQLOperatorClass
{
	PQLObject		obj;
	bool			defaultopclass;
	char			*intype;
	char			*accessmethod;
	PQLObject		family;
	char			*storagetype;

	/* operators/functions required by the access method */
	PQLOpAndFunc	opandfunc;

	char			*comment;
	char			*owner;
} PQLOperatorClass;

typedef struct PQLOperatorFamily
{
	PQLObject		obj;
	char			*accessmethod;

	/* operators/functions required by the access method */
	PQLOpAndFunc	opandfunc;

	char			*comment;
	char			*owner;
} PQLOperatorFamily;

PQLOperator *getOperators(PGconn *c, int *n);
PQLOperatorClass *getOperatorClasses(PGconn *c, int *n);
PQLOperatorFamily *getOperatorFamilies(PGconn *c, int *n);
void getOpFuncAttributes(PGconn *c, Oid o, PQLOpAndFunc *d);
int compareOperators(PQLOperator *a, PQLOperator *b);

void dumpDropOperator(FILE *output, PQLOperator *o);
void dumpDropOperatorClass(FILE *output, PQLOperatorClass *c);
void dumpDropOperatorFamily(FILE *output, PQLOperatorFamily *f);
void dumpCreateOperator(FILE *output, PQLOperator *o);
void dumpCreateOperatorClass(FILE *output, PQLOperatorClass *c);
void dumpCreateOperatorFamily(FILE *output, PQLOperatorFamily *f);
void dumpAlterOperator(FILE *output, PQLOperator *a, PQLOperator *b);
void dumpAlterOperatorClass(FILE *output, PQLOperatorClass *a,
							PQLOperatorClass *b);
void dumpAlterOperatorFamily(FILE *output, PQLOperatorFamily *a,
							 PQLOperatorFamily *b);

void freeOperators(PQLOperator *o, int n);
void freeOperatorClasses(PQLOperatorClass *c, int n);
void freeOperatorFamilies(PQLOperatorFamily *f, int n);

#endif	/* OPERATOR_H */
