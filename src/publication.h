/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef PUBLICATION_H
#define PUBLICATION_H

#include "common.h"

typedef struct PQLPublication
{
	Oid			oid;
	char		*pubname;
	PQLObject	*tables;
	int			ntables;
	bool		alltables;
	bool		pubinsert;
	bool		pubupdate;
	bool		pubdelete;
	bool		pubtruncate;
	char		*comment;
	char		*owner;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLPublication;

PQLPublication *getPublications(PGconn *c, int *n);
void getPublicationTables(PGconn *c, PQLPublication *p);
void getPublicationSecurityLabels(PGconn *c, PQLPublication *s);

void dumpDropPublication(FILE *output, PQLPublication *s);
void dumpCreatePublication(FILE *output, PQLPublication *s);
void dumpAlterPublication(FILE *output, PQLPublication *a, PQLPublication *b);

void freePublications(PQLPublication *s, int n);

#endif	/* PUBLICATION_H */

