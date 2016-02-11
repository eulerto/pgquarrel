#ifndef VIEW_H
#define VIEW_H

#include "common.h"

typedef struct PQLView
{
	PQLObject		obj;

	/* do not load iif view will be dropped */
	char			*viewdef;
	char			*checkoption;
	char			*reloptions;
	char			*comment;
	char			*owner;
} PQLView;

PQLView *getViews(PGconn *c, int *n);

void dumpDropView(FILE *output, PQLView v);
void dumpCreateView(FILE *output, PQLView v);
void dumpAlterView(FILE *output, PQLView a, PQLView b);

void freeViews(PQLView *v, int n);

#endif	/* VIEW_H */
