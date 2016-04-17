#ifndef USER_MAPPING_H
#define USER_MAPPING_H

#include "common.h"

typedef struct PQLUserMapping
{
	Oid			oid;
	Oid			useroid;	/* umuser */
	char		*user;
	char		*server;
	char		*options;
} PQLUserMapping;

PQLUserMapping *getUserMappings(PGconn *c, int *n);

void dumpDropUserMapping(FILE *output, PQLUserMapping *u);
void dumpCreateUserMapping(FILE *output, PQLUserMapping *u);
void dumpAlterUserMapping(FILE *output, PQLUserMapping *a,
							   PQLUserMapping *b);

int compareUserMappings(PQLUserMapping *a, PQLUserMapping *b);
void freeUserMappings(PQLUserMapping *u, int n);

#endif	/* USER_MAPPING_H */
