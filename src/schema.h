#ifndef SCHEMA_H
#define SCHEMA_H

#include "common.h"
#include "privileges.h"

typedef struct PQLSchema
{
	Oid		oid;
	char	*schemaname;
	char	*comment;
	char	*owner;
	char	*acl;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLSchema;

PQLSchema *getSchemas(PGconn *c, int *n);
void getSchemaSecurityLabels(PGconn *c, PQLSchema *s);

void dumpDropSchema(FILE *output, PQLSchema *s);
void dumpCreateSchema(FILE *output, PQLSchema *s);
void dumpAlterSchema(FILE *output, PQLSchema *a, PQLSchema *b);

void freeSchemas(PQLSchema *s, int n);

#endif	/* SCHEMA_H */
