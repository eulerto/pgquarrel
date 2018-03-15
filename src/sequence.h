/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef SEQUENCE_H
#define SEQUENCE_H

#include "common.h"
#include "privileges.h"

/*
 * Since commit 62e2a8dc2c7f6b1351a0385491933af969ed4265, postgres started to
 * define some integer limits independently from the system definitions. We
 * rely on some of those definitions then let's add it here to support old
 * postgres versions.
 */
#if PG_VERSION_NUM < 90500
#define PG_INT16_MIN	(-0x7FFF-1)
#define PG_INT16_MAX	(0x7FFF)
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#define PG_INT32_MAX	(0x7FFFFFFF)
#define PG_INT64_MIN	(-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)
#define PG_INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)
#endif

typedef struct PQLSequence
{
	PQLObject		obj;

	/* do not load iif sequence will be dropped */
	char			*startvalue;
	char			*incvalue;
	char			*minvalue;
	char			*maxvalue;
	char			*cache;
	bool			cycle;
	char			*comment;
	char			*owner;
	char			*acl;
	char			*typname;

	/* security labels */
	PQLSecLabel		*seclabels;
	int				nseclabels;
} PQLSequence;


PQLSequence *getSequences(PGconn *c, int *n);
void getSequenceAttributes(PGconn *c, PQLSequence *s);
void getSequenceSecurityLabels(PGconn *c, PQLSequence *s);

void dumpDropSequence(FILE *output, PQLSequence *s);
void dumpCreateSequence(FILE *output, PQLSequence *s);
void dumpAlterSequence(FILE *output, PQLSequence *a, PQLSequence *b);

void freeSequences(PQLSequence *s, int n);

#endif	/* SEQUENCE_H */
