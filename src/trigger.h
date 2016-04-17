#ifndef TRIGGER_H
#define TRIGGER_H

#include "common.h"

typedef struct PQLTrigger
{
	Oid				oid;
	char			*trgname;
	PQLObject		table;
	char			*trgdef;
	char			*comment;
} PQLTrigger;

PQLTrigger *getTriggers(PGconn *c, int *n);

void dumpDropTrigger(FILE *output, PQLTrigger *t);
void dumpCreateTrigger(FILE *output, PQLTrigger *t);
void dumpAlterTrigger(FILE *output, PQLTrigger *a, PQLTrigger *b);

void freeTriggers(PQLTrigger *t, int n);

#endif	/* TRIGGER_H */
