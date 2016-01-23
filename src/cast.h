#ifndef CAST_H
#define CAST_H

#include "common.h"

/*
 * Same as FirstNormalObjectId in access/transam.h. This value does not change
 * since a long time ago (2005). It is safe to use this value as a cutting
 * point for user oids x system oids.
 */
#define	PGQ_FIRST_USER_OID		16384

enum PQLCastMethod
{
	PGQ_CAST_METHOD_BINARY = 'b',
	PGQ_CAST_METHOD_FUNCTION = 'f',
	PGQ_CAST_METHOD_INOUT = 'i'
};

enum PQLCastContext
{
	PGQ_CAST_CONTEXT_ASSIGNMENT = 'a',
	PGQ_CAST_CONTEXT_EXPLICIT = 'e',
	PGQ_CAST_CONTEXT_IMPLICIT = 'i'
};

typedef struct PQLCast
{
	Oid			oid;
	char		*source;
	char		*target;
	char		method;
	char		*funcname;
	char		context;

	/* do not load iif cast will be dropped */
	char			*comment;
} PQLCast;

int compareCasts(PQLCast a, PQLCast b);
PQLCast *getCasts(PGconn *c, int *n);

void dumpDropCast(FILE *output, PQLCast d);
void dumpCreateCast(FILE *output, PQLCast d);
void dumpAlterCast(FILE *output, PQLCast a, PQLCast b);
#endif	/* CAST_H */
