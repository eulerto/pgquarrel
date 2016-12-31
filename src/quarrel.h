#ifndef PG_QUARREL_H
#define PG_QUARREL_H

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pg_config.h>			/* PG_VERSION_NUM */
#include <pg_config_manual.h>	/* NAMEDATALEN */

#include "common.h"

#define PGQ_NAME			"pgquarrel"
#define PGQ_VERSION			"0.2.0"
#define PGQ_SUPPORTED		90000	/* first supported version */
#define PGQ_SUPPORTED_STR	"9.0.0"


typedef struct PQLStatistic
{
	int		aggadded;
	int		aggremoved;
	int		castadded;
	int		castremoved;
	int		collationadded;
	int		collationremoved;
	int		conversionadded;
	int		conversionremoved;
	int		domainadded;
	int		domainremoved;
	int		evttrgadded;
	int		evttrgremoved;
	int		extensionadded;
	int		extensionremoved;
	int		fdwadded;
	int		fdwremoved;
	int		functionadded;
	int		functionremoved;
	int		indexadded;
	int		indexremoved;
	int		languageadded;
	int		languageremoved;
	int		matviewadded;
	int		matviewremoved;
	int		ruleadded;
	int		ruleremoved;
	int		schemaadded;
	int		schemaremoved;
	int		seqadded;
	int		seqremoved;
	int		serveradded;
	int		serverremoved;
	int		tableadded;
	int		tableremoved;
	int		trgadded;
	int		trgremoved;
	int		typeadded;
	int		typeremoved;
	int		usermappingadded;
	int		usermappingremoved;
	int		viewadded;
	int		viewremoved;
} PQLStatistic;

#endif	/* PG_QUARREL_H */
