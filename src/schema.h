#ifndef SCHEMA_H
#define SCHEMA_H

#include "common.h"

typedef struct PQLSchema
{
	char	*schemaname;
	char	*comment;
	char	*owner;
} PQLSchema;

PQLSchema *getSchemas(PGconn *c, int *n);
void dumpDropSchema(FILE *output, PQLSchema s);
void dumpCreateSchema(FILE *output, PQLSchema s);
void dumpAlterSchema(FILE *output, PQLSchema a, PQLSchema b);

#endif	/* SCHEMA_H */
