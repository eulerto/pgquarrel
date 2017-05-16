/*
 * mini-file.h
 * This file is part of mini, a library to parse INI files.
 *
 * Copyright (c) 2010, Francisco Javier Cuadrado <fcocuadrado@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the Francisco Javier Cuadrado nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __MINI_FILE_H__
#define __MINI_FILE_H__

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WINDOWS)

#ifndef	PGQ_DLLIMPORT
#define	PGQ_DLLIMPORT	__declspec(dllimport)
#endif

#else
#define	PGQ_DLLIMPORT
#endif

typedef struct _SectionData SectionData;
struct _SectionData
{
	char *key;
	char *value;
	SectionData *next;
};

typedef struct _Section Section;
struct _Section
{
	char *name;
	SectionData *data;
	Section *next;
};

typedef struct _MiniFile MiniFile;
struct _MiniFile
{
	char *file_name;
	Section *section;
};


MiniFile *mini_file_new(const char *file_name);

PGQ_DLLIMPORT void mini_file_free(MiniFile *mini_file);

MiniFile *mini_file_insert_section(MiniFile *mini_file, const char *section);

MiniFile *mini_file_insert_key_and_value(MiniFile *mini_file, const char *key,
        const char *value);

unsigned int mini_file_get_number_of_sections(MiniFile *mini_file);

unsigned int mini_file_get_number_of_keys(MiniFile *mini_file,
        const char *section);

PGQ_DLLIMPORT char *mini_file_get_value(MiniFile *mini_file, const char *section,
                          const char *key);

#endif /* __MINI_FILE_H__ */

