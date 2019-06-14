/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2018, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef VIEW_H
#define VIEW_H

#include "common.h"
#include "privileges.h"

typedef struct PQLView
{
	PQLObject		obj;

	/* do not load iif view will be dropped */
	char			*viewdef;
	char			*checkoption;
	char			*reloptions;
	char			*comment;
	char			*owner;
	char			*acl;

	/* security labels */
	PQLSecLabel		*seclabels;
	int				nseclabels;
} PQLView;

PQLView *getViews(PGconn *c, int *n);
void getViewSecurityLabels(PGconn *c, PQLView *v);
int compareViews(PQLView *a, PQLView *b);

void dumpDropView(FILE *output, PQLView *v);
void dumpCreateView(FILE *output, PQLView *v, bool orreplace);
void dumpAlterView(FILE *output, PQLView *a, PQLView *b);

void freeViews(PQLView *v, int n);

#endif	/* VIEW_H */
