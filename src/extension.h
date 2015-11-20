#ifndef EXTENSION_H
#define EXTENSION_H

#include "common.h"

typedef struct PQLExtension
{
	char	*extensionname;
	char	*schemaname;
	char	*version;
	bool	relocatable;
	char	*comment;
} PQLExtension;

PQLExtension *getExtensions(PGconn *c, int *n);
void dumpDropExtension(FILE *output, PQLExtension e);
void dumpCreateExtension(FILE *output, PQLExtension e);
void dumpAlterExtension(FILE *output, PQLExtension a, PQLExtension b);

#endif	/* EXTENSION_H */
