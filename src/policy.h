/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef POLICY_H
#define POLICY_H

#include "common.h"

typedef struct PQLPolicy
{
	Oid				oid;
	char			*polname;
	PQLObject		table;
	char			cmd;
	bool			permissive;
	char			*roles;
	char			*qual;
	char			*withcheck;
	char			*comment;
} PQLPolicy;

PQLPolicy *getPolicies(PGconn *c, int *n);

void dumpDropPolicy(FILE *output, PQLPolicy *p);
void dumpCreatePolicy(FILE *output, PQLPolicy *p);
void dumpAlterPolicy(FILE *output, PQLPolicy *a, PQLPolicy *b);

void freePolicies(PQLPolicy *t, int n);

#endif	/* POLICY_H */

