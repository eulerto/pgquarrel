/*
 * mini-readline.c
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

#include "mini-readline.h"


/**
 *  Reads a line from an opened file.
 *
 *  @param file An opened file.
 *  @return The return value is the readed line.
 *          The function returns NULL, if the file can't be readed.
 */
char *
mini_readline(FILE *file)
{
	char *line, *ret_line;
	size_t line_size = LINE_LEN;
	size_t line_len;

	assert(file != NULL);

	line = (char *) malloc(line_size * sizeof(char));
	if(line == NULL)
		return NULL;

	ret_line = fgets(line, line_size, file);
	if(ret_line == NULL)
	{
		free(line);
		return NULL;
	}

	line_len = strlen(line);

	while(line[line_len - 1] != EOL)
	{
		char *tmp_line;

		line_size *= 2;
		tmp_line = (char *) realloc(line, line_size * sizeof(char));
		if(tmp_line == NULL)
		{
			free(line);
			return NULL;
		}

		line = tmp_line;
		tmp_line = NULL;

		ret_line = fgets(&line[line_len], line_len + 2, file);
		if(ret_line == NULL)
		{
			free(line);
			return NULL;
		}

		line_len += strlen(ret_line);
	}

	return line;
}

