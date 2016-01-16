/*
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#include "common.h"

#include "c.h"
#include "parser/keywords.h"

#define	PG_KEYWORD(a,b,c) {a,0,c},

const ScanKeyword PQLScanKeywords[] =
{
#include "parser/kwlist.h"
};

const int NumPQLScanKeywords = lengthof(PQLScanKeywords);


static const char *logLevelTag[] =
{
	"FATAL",
	"ERROR",
	"WARNING",
	"DEBUG",
	"NOISE"
};


void
logGeneric(enum PQLLogLevel level, const char *fmt, ...)
{
	char		buf[2048];
	va_list		ap;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);

	if (level <= loglevel)
		fprintf(stderr, "%s %s\n", logLevelTag[level], buf);
}

int
compareRelations(PQLObject a, PQLObject b)
{
	int		c;

	c = strcmp(a.schemaname, b.schemaname);

	/* compare relation names iif schema names are equal */
	if (c == 0)
		c = strcmp(a.objectname, b.objectname);

	return c;
}

int
compareNamesAndRelations(PQLObject a, PQLObject b, char *aname, char *bname)
{
	int		c;

	c = strcmp(a.schemaname, b.schemaname);

	/* compare relation names iif schema names are equal */
	if (c == 0)
	{
		/* compare trigger/rule names iif schema.relation names are equal */
		c = strcmp(a.objectname, b.objectname);
		if (c == 0)
			c = strcmp(aname, bname);
	}

	return c;
}

/* borrowed from src/backend/parser/kwlookup.c */
const ScanKeyword *
ScanKeywordLookup(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords)
{
	int			len,
				i;
	char		word[NAMEDATALEN];
	const ScanKeyword *low;
	const ScanKeyword *high;

	len = strlen(text);
	/* We assume all keywords are shorter than NAMEDATALEN. */
	if (len >= NAMEDATALEN)
		return NULL;

	/*
	 * Apply an ASCII-only downcasing.  We must not use tolower() since it may
	 * produce the wrong translation in some locales (eg, Turkish).
	 */
	for (i = 0; i < len; i++)
	{
		char		ch = text[i];

		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';
		word[i] = ch;
	}
	word[len] = '\0';

	/*
	 * Now do a binary search using plain strcmp() comparison.
	 */
	low = keywords;
	high = keywords + (num_keywords - 1);
	while (low <= high)
	{
		const ScanKeyword *middle;
		int			difference;

		middle = low + (high - low) / 2;
		difference = strcmp(middle->name, word);
		if (difference == 0)
			return middle;
		else if (difference < 0)
			low = middle + 1;
		else
			high = middle - 1;
	}

	return NULL;
}

const char *
formatObjectIdentifier(char *s)
{
	bool	need_quotes = false;

	char	*p;
	char	*ret;
	int		i = 0;

	/* different rule for first character */
	if (!((s[0] >= 'a' && s[0] <= 'z') || s[0] == '_'))
		need_quotes = true;
	else
	{
		/* otherwise check the entire string */
		for (p = s; *p; p++)
		{
			if (!((*p >= 'a' && *p <= 'z')
					|| (*p >= '0' && *p <= '9')
					|| (*p == '_')))
			{
				need_quotes = true;
				break;
			}
		}
	}

	if (!need_quotes)
	{
		/* ScanKeywordLookup() was copied from postgres source code */
		const ScanKeyword *keyword = ScanKeywordLookup(s,
									 PQLScanKeywords,
									 NumPQLScanKeywords);

		if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
			need_quotes = true;
	}

	if (!need_quotes)
	{
		/* no quotes needed */
		ret = strdup(s);
	}
	else
	{
		ret = malloc(100 * sizeof(char));
		if (ret == NULL)
			logError("could not allocate memory");

		ret[i++] = '\"';
		for (p = s; *p; p++)
		{
			/* per SQL99, if quote is found, add another quote */
			if (*p == '\"')
				ret[i++] = '\"';
			ret[i++] = *p;
		}
		ret[i++] = '\"';
		ret[i] = '\0';
	}

	return ret;
}

void
appendStringList(stringList *sl, const char *s)
{
	stringListCell	*cell;

	cell = (stringListCell *) malloc(strlen(s) + 1);

	cell->next = NULL;
	cell->value = strdup(s);

	if (sl->tail)
		sl->tail->next = cell;
	else
		sl->head = cell;
	sl->tail = cell;
}

void
appendAllStringList(stringList *sl, char *s, const char *d)
{
	char	*token;

	token = strtok(s, d);
	while (token != NULL)
	{
		appendStringList(sl, token);
		token = strtok(NULL, d);
	}
}

void
printStringList(FILE *fd, stringList sl)
{
	stringListCell	*cell;

	for (cell = sl.head; cell; cell = cell->next)
		fprintf(fd, "%s", cell->value);
}

#ifdef _NOT_USED
bool
searchStringList(stringList *sl, const char *s)
{
	stringListCell	*cell;

	for (cell = sl->head; cell; cell = cell->next)
	{
		if (strcmp(cell->value, s) == 0)
			return true;
	}
	return false;
}
#endif

void
freeStringList(stringList *sl)
{
	stringListCell	*cell, *p;

	if (sl == NULL)
		return;

	cell = p = sl->head;
	while (cell)
	{
		p = cell->next;

		free(cell->value);
		free(cell);

		cell = p;
	}

	free(sl);
}
