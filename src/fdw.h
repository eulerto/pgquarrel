#ifndef FDW_H
#define FDW_H

#include "common.h"
#include "privileges.h"

typedef struct PQLForeignDataWrapper
{
	Oid			oid;
	char		*fdwname;
	PQLObject	handler;
	PQLObject	validator;

	char		*options;
	char		*owner;
	char		*acl;
	char		*comment;
} PQLForeignDataWrapper;

PQLForeignDataWrapper *getForeignDataWrappers(PGconn *c, int *n);

void dumpDropForeignDataWrapper(FILE *output, PQLForeignDataWrapper f);
void dumpCreateForeignDataWrapper(FILE *output, PQLForeignDataWrapper f);
void dumpAlterForeignDataWrapper(FILE *output, PQLForeignDataWrapper a,
							   PQLForeignDataWrapper b);

void freeForeignDataWrappers(PQLForeignDataWrapper *f, int n);

#endif	/* FDW_H */
