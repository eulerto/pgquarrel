/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef SERVER_H
#define SERVER_H

#include "common.h"
#include "privileges.h"

typedef struct PQLForeignServer
{
	Oid			oid;
	char		*servername;
	char		*serverfdw;
	char		*servertype;
	char		*serverversion;

	char		*options;
	char		*owner;
	char		*acl;
	char		*comment;
} PQLForeignServer;

PQLForeignServer *getForeignServers(PGconn *c, int *n);

void dumpDropForeignServer(FILE *output, PQLForeignServer *s);
void dumpCreateForeignServer(FILE *output, PQLForeignServer *s);
void dumpAlterForeignServer(FILE *output, PQLForeignServer *a,
							   PQLForeignServer *b);

void freeForeignServers(PQLForeignServer *s, int n);

#endif	/* SERVER_H */
