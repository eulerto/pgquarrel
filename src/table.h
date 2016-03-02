#ifndef TABLE_H
#define TABLE_H

#include "common.h"
#include "privileges.h"

typedef struct PQLTable
{
	PQLObject		obj;
	bool			unlogged;
	char			*tbspcname;

	/* do not load iif table will be dropped */
	PQLAttribute	*attributes;
	int				nattributes;

	PQLConstraint	*check;
	int				ncheck;

	PQLConstraint	*fk;
	int				nfk;

	PQLConstraint	pk;

	/* sequences that are owned by this table */
	PQLObject		*seqownedby;
	char			**attownedby;
	int				nownedby;

	char			*reloptions;
	char			relreplident;
	char			*relreplidentidx;

	char			*comment;
	char			*owner;
	char			*acl;

	/* security labels */
	PQLSecLabel		*seclabels;
	int				nseclabels;
} PQLTable;

PQLTable *getTables(PGconn *c, int *n);
void getTableAttributes(PGconn *c, PQLTable *t);
void getOwnedBySequences(PGconn *c, PQLTable *t);
void getCheckConstraints(PGconn *c, PQLTable *t, int n);
void getFKConstraints(PGconn *c, PQLTable *t, int n);
void getPKConstraints(PGconn *c, PQLTable *t, int n);
void getTableSecurityLabels(PGconn *c, PQLTable *t);

void dumpDropTable(FILE *output, PQLTable t);
void dumpCreateTable(FILE *output, PQLTable t);
void dumpAlterTable(FILE *output, PQLTable a, PQLTable b);

void freeTables(PQLTable *t, int n);

#endif	/* TABLE_H */
