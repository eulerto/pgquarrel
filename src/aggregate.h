/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef AGGREGATE_H
#define	AGGREGATE_H

#include "common.h"

typedef struct PQLAggregate
{
	PQLObject	obj;
	char		*arguments;
	char		*sfunc;
	char		*stype;
	char		*sspace;
	char		*finalfunc;
	bool		finalfuncextra;
	char		finalfuncmodify;
	char		*initcond;
	char		*msfunc;
	char		*minvfunc;
	char		*mstype;
	char		*msspace;
	char		*mfinalfunc;
	bool		mfinalfuncextra;
	char		mfinalfuncmodify;
	char		*minitcond;
	char		*sortop;
	char		parallel;
	bool		hypothetical;

	char		*comment;
	char		*owner;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLAggregate;

PQLAggregate *getAggregates(PGconn *c, int *n);
int compareAggregates(PQLAggregate *a, PQLAggregate *b);
void getAggregateSecurityLabels(PGconn *c, PQLAggregate *a);

void dumpDropAggregate(FILE *output, PQLAggregate *a);
void dumpCreateAggregate(FILE *output, PQLAggregate *a);
void dumpAlterAggregate(FILE *output, PQLAggregate *a, PQLAggregate *b);

void freeAggregates(PQLAggregate *a, int n);

#endif	/* AGGREGATE_H */
