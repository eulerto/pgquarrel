/*----------------------------------------------------------------------
 *
 * pgquarrel -- comparing database schemas
 *
 * Copyright (c) 2015-2020, Euler Taveira
 *
 * ---------------------------------------------------------------------
 */
#ifndef PG_QUARREL_H
#define PG_QUARREL_H

#include <errno.h>
#include "getopt_long.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pg_config.h>			/* PG_VERSION_NUM */
#include <pg_config_manual.h>	/* NAMEDATALEN */

#include "common.h"

#include <port.h>				/* simple_prompt */

#define PGQ_NAME			"pgquarrel"
#define PGQ_VERSION			"0.6.0"
#define PGQ_SUPPORTED		90000	/* first supported version */
#define PGQ_SUPPORTED_STR	"9.0.0"


typedef struct PQLStatistic
{
	int		amadded;
	int		amremoved;
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
	int		ftableadded;
	int		ftableremoved;
	int		functionadded;
	int		functionremoved;
	int		indexadded;
	int		indexremoved;
	int		languageadded;
	int		languageremoved;
	int		matviewadded;
	int		matviewremoved;
	int		operatoradded;
	int		operatorremoved;
	int		opfamilyadded;
	int		opfamilyremoved;
	int		opclassadded;
	int		opclassremoved;
	int		poladded;
	int		polremoved;
	int		procadded;
	int		procremoved;
	int		pubadded;
	int		pubremoved;
	int		ruleadded;
	int		ruleremoved;
	int		schemaadded;
	int		schemaremoved;
	int		seqadded;
	int		seqremoved;
	int		serveradded;
	int		serverremoved;
	int		stxadded;
	int		stxremoved;
	int		subadded;
	int		subremoved;
	int		tableadded;
	int		tableremoved;
	int		tsconfigadded;
	int		tsconfigremoved;
	int		tsdictadded;
	int		tsdictremoved;
	int		tsparseradded;
	int		tsparserremoved;
	int		tstemplateadded;
	int		tstemplateremoved;
	int		transformadded;
	int		transformremoved;
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
