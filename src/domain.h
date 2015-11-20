#ifndef DOMAIN_H
#define DOMAIN_H

#include "common.h"

typedef struct PQLDomain
{
	PQLObject		obj;

	/* do not load iif domain will be dropped */
	char			*domaindef;
	char			*collation;
	char			*ddefault;
	bool			notnull;
	PQLConstraint	*check;
	int				ncheck;
	char			*comment;
	char			*owner;
} PQLDomain;

PQLDomain *getDomains(PGconn *c, int *n);
void getDomainConstraints(PGconn *c, PQLDomain *d);

void dumpDropDomain(FILE *output, PQLDomain d);
void dumpCreateDomain(FILE *output, PQLDomain d);
void dumpAlterDomain(FILE *output, PQLDomain a, PQLDomain b);
#endif	/* DOMAIN_H */
