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
void getIndexAttributes(PGconn *c, PQLIndex *s);

void dumpDropIndex(FILE *output, PQLIndex s);
void dumpCreateIndex(FILE *output, PQLIndex s);
void dumpAlterIndex(FILE *output, PQLIndex a, PQLIndex b);

#endif	/* INDEX_H */
