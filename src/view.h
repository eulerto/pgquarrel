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
void getViewAttributes(PGconn *c, PQLView *v);
void dumpDropView(FILE *output, PQLView v);
void dumpCreateView(FILE *output, PQLView v);
void dumpAlterView(FILE *output, PQLView a, PQLView b);

#endif	/* VIEW_H */
