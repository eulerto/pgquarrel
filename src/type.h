/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef TYPE_H
#define TYPE_H

#include "common.h"
#include "privileges.h"

typedef struct PQLBaseType
{
	PQLObject	obj;
	int			length;
	char		*input;
	char		*output;
	char		*receive;
	char		*send;
	char		*modin;
	char		*modout;
	char		*analyze;
	bool		collatable;
	char		*typdefault;
	char		*category;
	bool		preferred;
	char		*delimiter;
	char		*align;
	char		*storage;
	bool		byvalue;
	char		*comment;
	char		*owner;
	char		*acl;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLBaseType;

/* TODO column comment */
typedef struct PQLAttrCompositeType
{
	char	*attname;
	char	*typname;
	char	*collschemaname;
	char	*collname;
} PQLAttrCompositeType;

typedef struct PQLCompositeType
{
	PQLObject				obj;
	PQLAttrCompositeType	*attributes;
	int						nattributes;
	char					*comment;
	char					*owner;
	char					*acl;

	/* security labels */
	PQLSecLabel				*seclabels;
	int						nseclabels;
} PQLCompositeType;

typedef struct PQLEnumType
{
	PQLObject	obj;
	char		**labels;
	int			nlabels;
	char		*comment;
	char		*owner;
	char		*acl;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLEnumType;

typedef struct PQLRangeType
{
	PQLObject	obj;
	char		*subtype;
	char		*opcschemaname;
	char		*opcname;
	bool		opcdefault;
	char		*collschemaname;
	char		*collname;
	char		*canonical;
	char		*diff;
	char		*comment;
	char		*owner;
	char		*acl;

	/* security labels */
	PQLSecLabel	*seclabels;
	int			nseclabels;
} PQLRangeType;

PQLBaseType *getBaseTypes(PGconn *c, int *n);
PQLCompositeType *getCompositeTypes(PGconn *c, int *n);
PQLEnumType *getEnumTypes(PGconn *c, int *n);
PQLRangeType *getRangeTypes(PGconn *c, int *n);

void getBaseTypeSecurityLabels(PGconn *c, PQLBaseType *t);
void getCompositeTypeSecurityLabels(PGconn *c, PQLCompositeType *t);
void getEnumTypeSecurityLabels(PGconn *c, PQLEnumType *t);
void getRangeTypeSecurityLabels(PGconn *c, PQLRangeType *t);

void dumpDropBaseType(FILE *output, PQLBaseType *t);
void dumpDropCompositeType(FILE *output, PQLCompositeType *t);
void dumpDropEnumType(FILE *output, PQLEnumType *t);
void dumpDropRangeType(FILE *output, PQLRangeType *t);

void dumpCreateBaseType(FILE *output, PQLBaseType *t);
void dumpCreateCompositeType(FILE *output, PQLCompositeType *t);
void dumpCreateEnumType(FILE *output, PQLEnumType *t);
void dumpCreateRangeType(FILE *output, PQLRangeType *t);

void dumpAlterBaseType(FILE *output, PQLBaseType *a, PQLBaseType *b);
void dumpAlterCompositeType(FILE *output, PQLCompositeType *a,
							PQLCompositeType *b);
void dumpAlterEnumType(FILE *output, PQLEnumType *a, PQLEnumType *b);
void dumpAlterRangeType(FILE *output, PQLRangeType *a, PQLRangeType *b);

void freeBaseTypes(PQLBaseType *t, int n);
void freeCompositeTypes(PQLCompositeType *t, int n);
void freeEnumTypes(PQLEnumType *t, int n);
void freeRangeTypes(PQLRangeType *t, int n);

#endif	/* TYPE_H */
