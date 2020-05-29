/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * common.c
 *     Common support routines for pgquarrel
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#include "common.h"


#if PG_VERSION_NUM >= 90600
#include "common/keywords.h"
#else
#include "parser/keywords.h"
#endif

#if PG_VERSION_NUM < 90600
#define	PG_KEYWORD(a,b,c) {a,0,c},

const ScanKeyword PQLScanKeywords[] =
{
#include "parser/kwlist.h"
};

const int NumPQLScanKeywords = lengthof(PQLScanKeywords);
#endif


static const char *logLevelTag[] =
{
	"FATAL",
	"ERROR",
	"WARNING",
	"DEBUG",
	"NOISE"
};


static stringListCell *intersectWithSortedLists(stringListCell *a,
		stringListCell *b, bool withvalue, bool changed);
static stringListCell *setDifferenceWithSortedLists(stringListCell *a,
		stringListCell *b, bool withvalue);

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
compareRelations(PQLObject *a, PQLObject *b)
{
	int		c;

	c = strcmp(a->schemaname, b->schemaname);

	/* compare relation names iif schema names are equal */
	if (c == 0)
		c = strcmp(a->objectname, b->objectname);

	return c;
}

int
compareNamesAndRelations(PQLObject *a, PQLObject *b, char *aname, char *bname)
{
	int		c;

	c = strcmp(a->schemaname, b->schemaname);

	/* compare relation names iif schema names are equal */
	if (c == 0)
	{
		/* compare trigger/rule names iif schema.relation names are equal */
		c = strcmp(a->objectname, b->objectname);
		if (c == 0)
			c = strcmp(aname, bname);
	}

	return c;
}

#if PG_VERSION_NUM < 90600
/*
 * This function was copied from postgres source code because it is not
 * available in a library. This is true for version 9.5 or prior.
 */
const ScanKeyword *
ScanKeywordLookup(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords)
{
	size_t		len;
	int			i;
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
#endif		/* <= 9.5 */

/*
 * Quote an identifier iif necessary. For quoting, consider the SQL rules and
 * also the list of PostgreSQL keywords. Those keywords come from PostgreSQL
 * version that pgquarrel was compiled with. Ensure that pgquarrel is compiled
 * with the latest PostgreSQL version to ensure that we don't have problems
 * with strings that become keywords. Returns an allocated string.
 */
char *
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
#if PG_VERSION_NUM >= 120000
		int kwnum = ScanKeywordLookup(s, &ScanKeywords);

		if (kwnum >= 0 && ScanKeywordCategories[kwnum] != UNRESERVED_KEYWORD)
			need_quotes = true;
#elif PG_VERSION_NUM >= 90600
		const ScanKeyword *keyword = ScanKeywordLookup(s,
									 ScanKeywords,
									 NumScanKeywords);

		if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
			need_quotes = true;
#else
		const ScanKeyword *keyword = ScanKeywordLookup(s,
									 PQLScanKeywords,
									 NumPQLScanKeywords);

		if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
			need_quotes = true;
#endif
	}

	if (!need_quotes)
	{
		/* no quotes needed */
		ret = strdup(s);
	}
	else
	{
		/*
		 * Maximum length for identifiers is NAMEDATALEN (64 bytes by default
		 * including trailing zero byte).
		 * */
		ret = malloc(NAMEDATALEN * sizeof(char));
		if (ret == NULL)
		{
			logError("could not allocate memory");
			exit(EXIT_FAILURE);
		}

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

/*
 * Build an ordered linked list from a comma-separated string. If there is no
 * options return NULL.
 */
stringList *
buildStringList(char *options)
{
	stringList		*sl;
	stringListCell	*x;
	char			*tmp;
	char			*p;

	char			*item;
	char			*nextitem;

	sl = (stringList *) malloc(sizeof(stringList));
	sl->head = sl->tail = NULL;

	/* no options, bail out */
	if (options == NULL)
	{
		logDebug("options is empty");
		return sl;
	}

	tmp = strdup(options);
	p = tmp;

	for (item = p; item; item = nextitem)
	{
		stringListCell	*sc;

		nextitem = strchr(item, ',');
		if (nextitem)
			*nextitem++ = '\0';

		/* left trim */
		while (isspace(*item))
			item++;

		sc = (stringListCell *) malloc(sizeof(stringListCell));
		sc->value = strdup(item);
		sc->next = NULL;

		logNoise("option: \"%s\"", item);

		/* add stringListCell to stringList in order */
		if (sl->tail)
		{
			stringListCell	*cur = sl->head;

			if (strcmp(cur->value, sc->value) > 0)
			{
				sc->next = cur;
				sl->head = sc;
			}
			else
			{
				while (cur != NULL)
				{
					if (cur == sl->tail)
					{
						cur->next = sc;
						sl->tail = sc;
						break;
					}

					if (strcmp(cur->value, sc->value) < 0 &&
							strcmp(cur->next->value, sc->value) >= 0)
					{
						sc->next = cur->next;
						cur->next = sc;
						break;
					}

					cur = cur->next;
				}
			}
		}
		else
		{
			sl->head = sc;
			sl->tail = sc;
		}
	}

	/* check the order */
	for (x = sl->head; x; x = x->next)
		logNoise("options in order: \"%s\"", x->value);

	free(tmp);

	return sl;
}

/*
 * Return a linked list of stringListCell that contains elements from 'a' that
 * is also in 'b'. If 'withvalue' is true, then strings are built with values
 * from 'b' else the list will contain only the options.
 */
static stringListCell *
intersectWithSortedLists(stringListCell *a, stringListCell *b, bool withvalue,
						 bool changed)
{
	stringListCell	*t;
	char			*c, *d;
	char			*tmpa, *tmpb;

	/* end of linked list */
	if (a == NULL || b == NULL)
		return NULL;

	/*
	 * If same option and value, call recursively incrementing both lists. We
	 * don't want to include option that doesn't change the value unless
	 * 'changed' is false.
	 */
	if (changed && strcmp(a->value, b->value) == 0)
		return intersectWithSortedLists(a->next, b->next, withvalue, changed);

	/* both string lists are not NULL */
	tmpa = strdup(a->value);
	tmpb = strdup(b->value);
	c = strtok(tmpa, "=");
	d = strtok(tmpb, "=");

	/* advance "smaller" string list and call recursively */
	if (strcmp(c, d) < 0)
	{
		/* avoid leaking temporary variables */
		free(tmpa);
		free(tmpb);

		return intersectWithSortedLists(a->next, b, withvalue, changed);
	}

	if (strcmp(c, d) > 0)
	{
		/* avoid leaking temporary variables */
		free(tmpa);
		free(tmpb);

		return intersectWithSortedLists(a, b->next, withvalue, changed);
	}

	/*
	 * If both options/values are equal, add it only if 'changed' is false.
	 * Same option and different values, add it.
	 */
	t = (stringListCell *) malloc(sizeof(stringListCell));
	if (withvalue)
		t->value = strdup(b->value);
	else
		t->value = strdup(d);

	/* avoid leaking temporary variables */
	free(tmpa);
	free(tmpb);

	/* advance both string lists and call recursively */
	t->next = intersectWithSortedLists(a->next, b->next, withvalue, changed);

	return t;
}

/*
 * Given two sorted linked lists (A, B), produce another linked list that is
 * the result of 'A minus B' i.e. elements that are only presented in A. If
 * there aren't elements, return NULL.
 */
static stringListCell *
setDifferenceWithSortedLists(stringListCell *a, stringListCell *b,
							 bool withvalue)
{
	stringListCell	*t;
	char			*c, *d;
	char			*tmpa, *tmpb;

	/* end of list */
	if (a == NULL)
		return NULL;

	/* latter list is NULL then transverse former list and build a linked list */
	if (b == NULL)
	{
		t = (stringListCell *) malloc(sizeof(stringListCell));

		if (withvalue)
			t->value = strdup(a->value);
		else
		{
			/* use a temporary variable because strtok() "destroy" the original string */
			tmpa = strdup(a->value);
			c = strtok(tmpa, "=");
			t->value = strdup(c);
			free(tmpa);
		}

		t->next = setDifferenceWithSortedLists(a->next, b, withvalue);

		return t;
	}

	/* both string list are not NULL */
	tmpa = strdup(a->value);
	tmpb = strdup(b->value);
	c = strtok(tmpa, "=");
	d = strtok(tmpb, "=");

	/* advance latter list and call recursively */
	if (strcmp(c, d) > 0)
	{
		/* avoid leaking temporary variables */
		free(tmpa);
		free(tmpb);

		return setDifferenceWithSortedLists(a, b->next, withvalue);
	}

	/* advance both string lists and call recursively */
	if (strcmp(c, d) == 0)
	{
		/* avoid leaking temporary variables */
		free(tmpa);
		free(tmpb);

		return setDifferenceWithSortedLists(a->next, b->next, withvalue);
	}

	/* executed only if A value is not in B */
	t = (stringListCell *) malloc(sizeof(stringListCell));
	if (withvalue)
		t->value = strdup(a->value);
	else
		t->value = strdup(c);

	/* avoid leaking temporary variables */
	free(tmpa);
	free(tmpb);

	t->next = setDifferenceWithSortedLists(a->next, b, withvalue);

	return t;
}

/*
 * Return a linked list that contains elements according to specified set
 * operation (setop). If there aren't options, return NULL.
 */
stringList *
setOperationOptions(char *a, char *b, int setop, bool withvalue, bool changed)
{
	stringList		*first, *second;
	stringList		*ret = NULL;
	stringListCell	*headitem = NULL;

	logNoise("options: set operation %d", setop);

	/* if a is NULL, there is neither intersection nor complement (except) */
	if (a == NULL)
		return NULL;

	/* if b is NULL, there isn't intersection */
	if (setop == PGQ_INTERSECT && b == NULL)
		return NULL;

	first = buildStringList(a);
	second = buildStringList(b);

	if (setop == PGQ_INTERSECT)
		headitem = intersectWithSortedLists(first->head, second->head, withvalue,
											changed);
	else if (setop == PGQ_SETDIFFERENCE)
		headitem = setDifferenceWithSortedLists(first->head, second->head, withvalue);
	else
		logError("set operation not supported");

	/* build a linked list */
	if (headitem)
	{
		stringListCell	*p;

		ret = (stringList *) malloc(sizeof(stringList));

		/* linked list head */
		ret->head = headitem;

		/* find linked list tail */
		p = headitem;
		while (p->next)
			p = p->next;
		ret->tail = p;
	}

	/* free temporary lists */
	if (first)
		freeStringList(first);
	if (second)
		freeStringList(second);

	return ret;
}

/*
 * Return an allocated string that contains comma-separated options. Return
 * NULL if linked list is NULL.
 */
char *
printOptions(stringList *sl)
{
	char			*list = NULL;
	stringListCell	*p;
	bool			firstitem = true;

	if (sl)
	{
		size_t	listlen;
		size_t	n = 0;

		/* allocate memory for at list one parameter (based on autovac parameters) */
		listlen = 40;
		list = (char *) malloc(listlen * sizeof(char));

		/* build a list like 'a=10, b=20, c=30' or 'a, b, c' */
		for (p = sl->head; p; p = p->next)
		{
			size_t	newlen;

			/*
			 * String space including new option and separator. Don't forget
			 * the null-character.
			 */
			newlen = n + strlen(p->value) + 2 + 1;
			if (newlen > listlen)
			{
				logNoise("allocate more memory (was %d ; is %d)", listlen, newlen);

				listlen = newlen;
				list = (char *) realloc(list, listlen);
				if (list == NULL)
				{
					logError("could not allocate memory");
					exit(EXIT_FAILURE);
				}
			}

			if (firstitem)
				firstitem = false;
			else
			{
				/*
				 * If it is not the first item, add comma and space before it.
				 * Copy 3 characters from destination to source, although only
				 * 2 (comma and space) will be added to list. Don't forget the
				 * null-character.
				 */
				strncpy(list + n, ", ", 3);
				n += 2;
			}

			strcpy(list + n, p->value);
			n += strlen(p->value);
		}
	}

	if (list)
		logNoise("options: %s", list);

	return list;
}
