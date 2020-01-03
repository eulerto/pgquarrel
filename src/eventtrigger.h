/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef EVENTTRIGGER_H
#define EVENTTRIGGER_H

#include "common.h"

typedef struct PQLEventTrigger
{
	Oid		oid;
	char	*trgname;
	char	*event;
	char	*tags;
	char	*functionname;
	char	enabled;
	char	*comment;
	char	*owner;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLEventTrigger;

PQLEventTrigger *getEventTriggers(PGconn *c, int *n);
void getEventTriggerSecurityLabels(PGconn *c, PQLEventTrigger *e);

void dumpDropEventTrigger(FILE *output, PQLEventTrigger *e);
void dumpCreateEventTrigger(FILE *output, PQLEventTrigger *e);
void dumpAlterEventTrigger(FILE *output, PQLEventTrigger *a,
						   PQLEventTrigger *b);

void freeEventTriggers(PQLEventTrigger *e, int n);

#endif	/* EVENTTRIGGER_H */
