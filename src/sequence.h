#ifndef SEQUENCE_H
#define SEQUENCE_H

#include "common.h"

#define MINIMUM_SEQUENCE_VALUE "-9223372036854775807"
#define MAXIMUM_SEQUENCE_VALUE "9223372036854775807"

typedef struct PQLSequence
{
	PQLObject		obj;

	/* do not load iif sequence will be dropped */
	char			*startvalue;
	char			*incvalue;
	char			*minvalue;
	char			*maxvalue;
	char			*cache;
	bool			cycle;
	char			*comment;
	char			*owner;
} PQLSequence;


PQLSequence *getSequences(PGconn *c, int *n);
void getSequenceAttributes(PGconn *c, PQLSequence *s);

void dumpDropSequence(FILE *output, PQLSequence s);
void dumpCreateSequence(FILE *output, PQLSequence s);
void dumpAlterSequence(FILE *output, PQLSequence a, PQLSequence b);

#endif	/* SEQUENCE_H */
