/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef SUBSCRIPTION_H
#define SUBSCRIPTION_H

#include "common.h"

typedef struct PQLSubPublication
{
	char		*pubname;
} PQLSubPublication;

typedef struct PQLSubscription
{
	Oid					oid;
	char				*subname;
	char				*conninfo;
	char				*slotname;
	char				*synccommit;
	PQLSubPublication	*publications;
	int					npublications;
	bool				enabled;
	char				*comment;
	char				*owner;

	/* security labels */
	PQLSecLabel			*seclabels;
	int					nseclabels;
} PQLSubscription;

PQLSubscription *getSubscriptions(PGconn *c, int *n);
void getSubscriptionPublications(PGconn *c, PQLSubscription *p);
void getSubscriptionSecurityLabels(PGconn *c, PQLSubscription *s);

void dumpDropSubscription(FILE *output, PQLSubscription *s);
void dumpCreateSubscription(FILE *output, PQLSubscription *s);
void dumpAlterSubscription(FILE *output, PQLSubscription *a,
						   PQLSubscription *b);

void freeSubscriptions(PQLSubscription *s, int n);

#endif	/* SUBSCRIPTION_H */

