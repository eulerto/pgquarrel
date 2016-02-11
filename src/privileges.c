#include "privileges.h"

/*
 * GRANT ON TABLE
 * GRANT ON SEQUENCE
 * GRANT ON DOMAIN
 * GRANT ON FUNCTION
 * GRANT ON LANGUAGE
 * GRANT ON SCHEMA
 * GRANT ON TYPE
 * GRANT ON DATABASE
 * GRANT ON FOREIGN DATA WRAPPER
 * GRANT ON FOREIGN SERVER
 * GRANT ON TABLESPACE
 *
 * REVOKE ON TABLE
 * REVOKE ON SEQUENCE
 * REVOKE ON DOMAIN
 * REVOKE ON FUNCTION
 * REVOKE ON LANGUAGE
 * REVOKE ON SCHEMA
 * REVOKE ON TYPE
 * REVOKE ON DATABASE
 * REVOKE ON FOREIGN DATA WRAPPER
 * REVOKE ON FOREIGN SERVER
 * REVOKE ON TABLESPACE
 *
 * TODO
 *
 * GRANT (columns) ON TABLE
 * REVOKE (columns) ON TABLE
 */

/*
 * We do not check for privilege correctness. We assume that the object accepts
 * all privileges informed. This function returns a allocated string that is a
 * list of the described privileges.
 */
char *
formatPrivileges(char *s)
{
#define	MAX_KEYWORD_LEN		10	/* maximum keyword length */

	char	*ret, *ptr;
	int		len;
	bool	first_item = true;
	int		i;

	if (s == NULL)
		return NULL;

	len = strlen(s);
	ret = (char *) malloc((len * MAX_KEYWORD_LEN + (len - 1) * 2) * sizeof(char));
	ret[0] = '\0';
	ptr = ret;

	for (i = 0; i < len; i++)
	{
		if (first_item)
		{
			first_item = false;
		}
		else
		{
			strncpy(ptr, ", ", 2);
			ptr += 2;
		}

		switch (s[i])
		{
			case 'r':	/* SELECT */
				strncpy(ptr, "SELECT", 6);
				ptr += 6;
				break;
			case 'U':	/* USAGE */
				strncpy(ptr, "USAGE", 5);
				ptr += 5;
				break;
			case 'a':	/* INSERT */
				strncpy(ptr, "INSERT", 6);
				ptr += 6;
				break;
			case 'x':	/* REFERENCES */
				strncpy(ptr, "REFERENCES", 10);
				ptr += 10;
				break;
			case 'd':	/* DELETE */
				strncpy(ptr, "DELETE", 6);
				ptr += 6;
				break;
			case 't':	/* TRIGGER */
				strncpy(ptr, "TRIGGER", 7);
				ptr += 7;
				break;
			case 'D':	/* TRUNCATE */
				strncpy(ptr, "TRUNCATE", 8);
				ptr += 8;
				break;
			case 'w':	/* UPDATE */
				strncpy(ptr, "UPDATE", 6);
				ptr += 6;
				break;
			case 'X':	/* EXECUTE */
				strncpy(ptr, "EXECUTE", 7);
				ptr += 7;
				break;
			case 'C':	/* CREATE */
				strncpy(ptr, "CREATE", 6);
				ptr += 6;
				break;
			case 'c':	/* CONNECT */
				strncpy(ptr, "CONNECT", 7);
				ptr += 7;
				break;
			case 'T':	/* TEMPORARY */
				strncpy(ptr, "TEMPORARY", 9);
				ptr += 9;
				break;
		}
	}

	*ptr = '\0';

	logNoise("privileges: %s", ret);

	return ret;
}

aclItem *
splitACLItem(char *a)
{
	aclItem	*ai;

	char	*ptr;
	char	*nextptr;
	int		len;

	ai = (aclItem *) malloc(sizeof(aclItem));
	ai->next = NULL;

	ptr = a;

	/* grantee */
	nextptr = strchr(ptr, '=');
	if (nextptr)
		*nextptr++ = '\0';
	len = strlen(ptr);
	/* if len is zero then grantee is PUBLIC */
	if (len == 0)
	{
		ai->grantee = (char *) malloc(7 * sizeof(char));
		strcpy(ai->grantee, "PUBLIC");
		ai->grantee[6] = '\0';
	}
	else
	{
		ai->grantee = (char *) malloc((len + 1) * sizeof(char));
		strncpy(ai->grantee, ptr, len + 1);
	}

	ptr = nextptr;

	/* privileges */
	nextptr = strchr(ptr, '/');
	if (nextptr)
		*nextptr++ = '\0';
	len = strlen(ptr);
	ai->privileges = (char *) malloc((len + 1) * sizeof(char));
	strncpy(ai->privileges, ptr, len + 1);

	ptr = nextptr;

	/* grantor */
	len = strlen(ptr);
	ai->grantor = (char *) malloc((len + 1) * sizeof(char));
	strncpy(ai->grantor, ptr, len + 1);

	logNoise("grantee: %s ; grantor: %s ; privileges: %s", ai->grantee, ai->grantor, ai->privileges);

	return ai;
}

void
freeACLItem(aclItem *ai)
{
	free(ai->grantee);
	free(ai->privileges);
	free(ai->grantor);
	free(ai);
}

aclList *
buildACL(char *acl)
{
	aclList	*al;
	aclItem *x;
	char	*tmp;
	char	*p;

	char	*item;
	char	*nextitem;
	int		len;

	if (acl == NULL)
	{
		logNoise("acl is empty");
		return NULL;
	}
	logNoise("acl: \"%s\"", acl);

	al = (aclList *) malloc(sizeof(aclList));
	al->head = al->tail = NULL;

	len = strlen(acl);

	/* use a temporary variable to avoid changing the original acl */
	tmp = strdup(acl);
	p = tmp;

	/*
	 * Let's strip Postgres array delimiters { }
	 * The } character is removed first because we'll increment the pointer
	 * above.
	 */
	if (tmp[len - 1] != '}')	/* can't happen */
	{
		logWarning("mal formed ACL \"%s\" (last character is \"%c\")", tmp, tmp[len - 1]);
		return NULL;
	}
	p[len - 1] = '\0';

	if (tmp[0] != '{')	/* can't happen */
	{
		logWarning("mal formed ACL \"%s\" (first character is \"%c\")", tmp, tmp[0]);
		return NULL;
	}
	p++;


	for (item = p; item; item = nextitem)
	{
		aclItem		*ai;

		nextitem = strchr(item, ',');
		if (nextitem)
			*nextitem++ = '\0';

		logNoise("ACL item: %s", item);

		ai = splitACLItem(item);

		/* add aclItem to aclList in order */
		if (al->tail)
		{
			aclItem	*cur = al->head;

			if (strcmp(cur->grantee, ai->grantee) > 0)
			{
				ai->next = cur;
				al->head = ai;
			}
			else
			{
				while (cur != NULL)
				{
					if (cur == al->tail)
					{
						cur->next = ai;
						al->tail = ai;
						break;
					}

					if (strcmp(cur->grantee, ai->grantee) < 0 && strcmp(cur->next->grantee, ai->grantee) >= 0)
					{
						ai->next = cur->next;
						cur->next = ai;
						break;
					}

					cur = cur->next;
				}
			}
		}
		else
		{
			al->head = ai;
			al->tail = ai;
		}
	}

	/* check the order */
	for (x = al->head; x; x = x->next)
		logNoise("grantee: %s ; privs: %s", x->grantee, x->privileges);

	free(tmp);

	return al;
}

void
freeACL(aclList *al)
{
	aclItem	*ai;

	if (al == NULL)
		return;

	for (ai = al->head; ai; ai = ai->next)
		freeACLItem(ai);

	free(al);
}

/*
 * Return an allocated string that contains characters available in string a
 * but not in b.
 */
char *
diffPrivileges(char *a, char *b)
{
#define	MAX_ACL_LEN		16

	char	*r;
	char	*tmpa, *tmpb, *tmpr;

	tmpa = a;
	tmpb = b;

	r = (char *) malloc((MAX_ACL_LEN + 1) * sizeof(char));
	tmpr = r;
	r[0] = '\0';

	if (b == NULL)
	{
		strncpy(r, a, MAX_ACL_LEN);
	}
	else if (tmpa != NULL)
	{
		while (*tmpa != '\0')
		{
			bool	notfound = true;

			while (*tmpb != '\0')
			{
				if (*tmpa == *tmpb)
				{
					notfound = false;
					break;
				}

				tmpb++;
			}

			if (notfound)
				*tmpr++ = *tmpa;

			tmpa++;
			tmpb = b;	/* start again */
		}
	}

	/* if there is no differences, then return NULL */
	if (r[0] == '\0')
	{
		free(r);
		r = NULL;
	}

	logNoise("a: %s ; b: %s ; intersection: \"%s\"", a, b, r);

	return r;
}

void
dumpGrant(FILE *output, int objecttype, PQLObject a, char *privs, char *grantee, char *extra)
{
	char	*p;

	/* nothing to be done */
	if (privs == NULL)
		return;

	p = formatPrivileges(privs);

	fprintf(output, "\n\n");
	fprintf(output, "GRANT %s ON ", p);

	switch (objecttype)
	{
		/* (foreign) table, (mat) view */
		case PGQ_TABLE:
			fprintf(output, "TABLE");
			break;
		case PGQ_SEQUENCE:
			fprintf(output, "SEQUENCE");
			break;
		case PGQ_DATABASE:
			fprintf(output, "DATABASE");
			break;
		case PGQ_DOMAIN:
			fprintf(output, "DOMAIN");
			break;
		case PGQ_FOREIGN_DATA_WRAPPER:
			fprintf(output, "FOREIGN DATA WRAPPER");
			break;
		case PGQ_FOREIGN_SERVER:
			fprintf(output, "FOREIGN SERVER");
			break;
		case PGQ_FUNCTION:
			fprintf(output, "FUNCTION");
			break;
		case PGQ_LANGUAGE:
			fprintf(output, "LANGUAGE");
			break;
		case PGQ_SCHEMA:
			fprintf(output, "SCHEMA");
			break;
		case PGQ_TABLESPACE:
			fprintf(output, "TABLESPACE");
			break;
		case PGQ_TYPE:
			fprintf(output, "TYPE");
			break;
	}

	/* function arguments? */
	if (objecttype == PGQ_FUNCTION && extra != NULL)
	{
		fprintf(output, " %s.%s(%s) TO %s;",
			formatObjectIdentifier(a.schemaname),
			formatObjectIdentifier(a.objectname),
			extra,
			grantee);
	}
	else if (objecttype == PGQ_DATABASE || objecttype == PGQ_FOREIGN_DATA_WRAPPER ||
			objecttype == PGQ_FOREIGN_SERVER || objecttype == PGQ_LANGUAGE ||
			objecttype == PGQ_SCHEMA || objecttype == PGQ_TABLESPACE)
	{
		fprintf(output, " %s TO %s;",
			formatObjectIdentifier(a.objectname),
			grantee);
	}
	else
	{
		fprintf(output, " %s.%s TO %s;",
			formatObjectIdentifier(a.schemaname),
			formatObjectIdentifier(a.objectname),
			grantee);
	}

	free(p);
}

void
dumpRevoke(FILE *output, int objecttype, PQLObject a, char *privs, char *grantee, char *extra)
{
	char	*p;

	/* nothing to be done */
	if (privs == NULL)
		return;

	p = formatPrivileges(privs);

	fprintf(output, "\n\n");
	fprintf(output, "REVOKE %s ON ", p);

	switch (objecttype)
	{
		/* (foreign) table, (mat) view */
		case PGQ_TABLE:
			fprintf(output, "TABLE");
			break;
		case PGQ_SEQUENCE:
			fprintf(output, "SEQUENCE");
			break;
		case PGQ_DATABASE:
			fprintf(output, "DATABASE");
			break;
		case PGQ_DOMAIN:
			fprintf(output, "DOMAIN");
			break;
		case PGQ_FOREIGN_DATA_WRAPPER:
			fprintf(output, "FOREIGN DATA WRAPPER");
			break;
		case PGQ_FOREIGN_SERVER:
			fprintf(output, "FOREIGN SERVER");
			break;
		case PGQ_FUNCTION:
			fprintf(output, "FUNCTION");
			break;
		case PGQ_LANGUAGE:
			fprintf(output, "LANGUAGE");
			break;
		case PGQ_SCHEMA:
			fprintf(output, "SCHEMA");
			break;
		case PGQ_TABLESPACE:
			fprintf(output, "TABLESPACE");
			break;
		case PGQ_TYPE:
			fprintf(output, "TYPE");
			break;
	}

	/* function arguments? */
	if (objecttype == PGQ_FUNCTION && extra != NULL)
	{
		fprintf(output, " %s.%s(%s) FROM %s;",
			formatObjectIdentifier(a.schemaname),
			formatObjectIdentifier(a.objectname),
			extra,
			grantee);
	}
	else if (objecttype == PGQ_DATABASE || objecttype == PGQ_FOREIGN_DATA_WRAPPER ||
			objecttype == PGQ_FOREIGN_SERVER || objecttype == PGQ_LANGUAGE ||
			objecttype == PGQ_SCHEMA || objecttype == PGQ_TABLESPACE)
	{
		fprintf(output, " %s FROM %s;",
			formatObjectIdentifier(a.objectname),
			grantee);
	}
	else
	{
		fprintf(output, " %s.%s FROM %s;",
			formatObjectIdentifier(a.schemaname),
			formatObjectIdentifier(a.objectname),
			grantee);
	}

	free(p);
}

void
dumpGrantAndRevoke(FILE *output, int objecttype, PQLObject a, PQLObject b, char *acla, char *aclb, char *extra)
{
	aclList		*ala;
	aclList		*alb;
	aclItem		*tmpa = NULL;
	aclItem		*tmpb = NULL;

	ala = buildACL(acla);
	alb = buildACL(aclb);

	if (ala != NULL)
		tmpa = ala->head;
	if (alb != NULL)
		tmpb = alb->head;

	while (ala != NULL || alb != NULL)
	{
		/* End of aclList ala. Print GRANT for aclList alb until its end. */
		if (tmpa == NULL)
		{
			logDebug("grant to %s: server2 (end)", tmpb->grantee);

			dumpGrant(output, objecttype, b, tmpb->privileges, tmpb->grantee, ((objecttype == PGQ_FUNCTION) ? extra : NULL));
			tmpb = tmpb->next;
		}
		/* End of aclList alb. Print REVOKE for aclList ala until its end. */
		else if (tmpb == NULL)
		{
			logDebug("revoke from %s: server1 (end)", tmpa->grantee);

			dumpRevoke(output, objecttype, a, tmpa->privileges, tmpa->grantee, ((objecttype == PGQ_FUNCTION) ? extra : NULL));
			tmpa = tmpa->next;
		}
		else if (strcmp(tmpa->grantee, tmpb->grantee) == 0)
		{
			char	*privs;

			logDebug("grant/revoke %s: server1 server2", tmpa->grantee);

			privs = diffPrivileges(tmpa->privileges, tmpb->privileges);
			dumpRevoke(output, objecttype, a, privs, tmpa->grantee, ((objecttype == PGQ_FUNCTION) ? extra : NULL));
			if (privs != NULL)
				free(privs);

			privs = diffPrivileges(tmpb->privileges, tmpa->privileges);
			dumpGrant(output, objecttype, b, privs, tmpb->grantee, ((objecttype == PGQ_FUNCTION) ? extra : NULL));
			if (privs != NULL)
				free(privs);

			tmpa = tmpa->next;
			tmpb = tmpb->next;
		}
		else if (strcmp(tmpa->grantee, tmpb->grantee) < 0)
		{
			logDebug("revoke from %s: server1", tmpa->grantee);

			dumpRevoke(output, objecttype, a, tmpa->privileges, tmpa->grantee, ((objecttype == PGQ_FUNCTION) ? extra : NULL));
			tmpa = tmpa->next;
		}
		else if (strcmp(tmpa->grantee, tmpb->grantee) > 0)
		{
			logDebug("grant to %s: server2", tmpb->grantee);

			dumpGrant(output, objecttype, b, tmpb->privileges, tmpb->grantee, ((objecttype == PGQ_FUNCTION) ? extra : NULL));
			tmpb = tmpb->next;
		}

		/* both lists ended then exit */
		if (tmpa == NULL && tmpb == NULL)
			break;
	}

	/* free temporary lists */
	freeACL(ala);
	freeACL(alb);
}
