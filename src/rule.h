#ifndef RULE_H
#define RULE_H

#include "common.h"

typedef struct PQLRule
{
	char		*rulename;
	PQLObject	table;
	char		*ruledef;
	char		*comment;
} PQLRule;

PQLRule *getRules(PGconn *c, int *n);
void dumpDropRule(FILE *output, PQLRule s);
void dumpCreateRule(FILE *output, PQLRule s);
void dumpAlterRule(FILE *output, PQLRule a, PQLRule b);

#endif	/* RULE_H */
