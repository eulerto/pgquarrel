#ifndef INDEX_H
#define INDEX_H

#include "common.h"

typedef struct PQLIndex
{
	PQLObject		obj;
	char			*tbspcname;

	/* do not load iif index will be dropped */
	char			*indexdef;
	char			*reloptions;
	char			*comment;
} PQLIndex;


PQLIndex *getIndexes(PGconn *c, int *n);

void dumpDropIndex(FILE *output, PQLIndex s);
void dumpCreateIndex(FILE *output, PQLIndex s);
void dumpAlterIndex(FILE *output, PQLIndex a, PQLIndex b);

void freeIndexes(PQLIndex *i, int n);

#endif	/* INDEX_H */
