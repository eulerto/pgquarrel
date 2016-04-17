#ifndef RULE_H
#define RULE_H

#include "common.h"

typedef struct PQLRule
{
	Oid			oid;
	char		*rulename;
	PQLObject	table;
	char		*ruledef;
	char		*comment;
} PQLRule;

PQLRule *getRules(PGconn *c, int *n);
void dumpDropRule(FILE *output, PQLRule *r);
void dumpCreateRule(FILE *output, PQLRule *r);
void dumpAlterRule(FILE *output, PQLRule *a, PQLRule *b);

void freeRules(PQLRule *r, int n);

#endif	/* RULE_H */
