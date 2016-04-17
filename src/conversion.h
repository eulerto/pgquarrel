#ifndef CONVERSION_H
#define	CONVERSION_H

#include "common.h"

typedef struct PQLConversion
{
	PQLObject	obj;

	char		*owner;
	char		*forencoding;
	char		*toencoding;
	char		*funcname;
	bool		convdefault;
	char		*comment;
} PQLConversion;

PQLConversion *getConversions(PGconn *c, int *n);

void dumpDropConversion(FILE *output, PQLConversion *c);
void dumpCreateConversion(FILE *output, PQLConversion *c);
void dumpAlterConversion(FILE *output, PQLConversion *a, PQLConversion *b);

void freeConversions(PQLConversion *c, int n);

#endif	/* CONVERSION_H */
