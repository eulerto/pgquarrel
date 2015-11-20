/*
 * mini-strip.c
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

#include "mini-strip.h"


/**
 *  Strips left whitespaces.
 *
 *  @param string String to be stripped.
 *  @return The return value is the stripped string.
 */
char *
mini_lstrip(char *string)
{
	char *p;

	/* String can't be NULL */
	assert(string != NULL);

	/* Search the first non whitespace character from left to right */
	for(p = string; (p != NULL) && isspace(*p); p++)
		;

	return p;
}

/**
 *  Strips right whitespaces.
 *
 *  @param string String to be stripped.
 *  @return The return value is the stripped string.
 */
char *
mini_rstrip(char *string)
{
	char *p;
	int pos;
	size_t len;

	/* String can't be NULL */
	assert(string != NULL);

	p = string;
	len = strlen(string);

	/* Search the first non whitespace character from right to left */
	for(pos = len - 1; (pos >= 0) && isspace(p[pos]); pos--)
		;

	if((pos >= 0) && !isspace(p[pos]))
		p[pos + 1] = '\0';

	return string;
}

/**
 *  Strips left and right whitespaces.
 *
 *  @param string String to be stripped.
 *  @return The return value is the stripped string.
 */
char *
mini_strip(char *string)
{
	char *ret;

	/* String can't be NULL */
	assert(string != NULL);

	ret = mini_lstrip(string);
	ret = mini_rstrip(ret);

	return ret;
}

