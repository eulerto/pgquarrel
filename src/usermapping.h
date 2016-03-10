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

void dumpDropUserMapping(FILE *output, PQLUserMapping s);
void dumpCreateUserMapping(FILE *output, PQLUserMapping s);
void dumpAlterUserMapping(FILE *output, PQLUserMapping a,
							   PQLUserMapping b);

void freeUserMappings(PQLUserMapping *s, int n);

#endif	/* USER_MAPPING_H */
