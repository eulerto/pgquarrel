/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef STATISTICS_H
#define STATISTICS_H

#include "common.h"

typedef struct PQLStatistics
{
	PQLObject		obj;
	char			*stxdef;
	char			*comment;
	char			*owner;
} PQLStatistics;

PQLStatistics *getStatistics(PGconn *c, int *n);

void dumpDropStatistics(FILE *output, PQLStatistics *s);
void dumpCreateStatistics(FILE *output, PQLStatistics *s);
void dumpAlterStatistics(FILE *output, PQLStatistics *a, PQLStatistics *b);

void freeStatistics(PQLStatistics *s, int n);

#endif	/* STATISTICS_H */
