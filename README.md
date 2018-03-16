[![Build Status](https://travis-ci.org/eulerto/pgquarrel.svg?branch=master)](https://travis-ci.org/eulerto/pgquarrel)

Introduction
============

**pgquarrel** is a program that compares [PostgreSQL](http://www.postgresql.org/) database schemas (DDL).

Given two database connections, it output a file that represent the difference between schemas. It means that if you run the output file into the *from* database, it'll have the same schema as the *to* database. The main use case is to deploy database changes into testing, staging or production environment.

**pgquarrel** does not rely on another tool (such as **pg\_dump**) instead it connects directly to PostgreSQL server, obtain meta data from catalog, compare objects and output the commands necessary to turn *from* database into *to* database.

It could work with different PostgreSQL versions. The generated file could not work as expected if the *to* PostgreSQL version is greater than *from* PostgreSQL version. That's because the tool could generate commands that does not exist in a prior PostgreSQL version.


It works with different operating systems. It was tested on Linux, FreeBSD, and Windows.

Installation
============

**pgquarrel** is distributed as a source package and can be downloaded at [GitHub](http://github.com/eulerto/pgquarrel). The installation steps depend on your operating system.

You can also keep up with the latest fixes and features cloning the Git repository.

```
$ git clone https://github.com/eulerto/pgquarrel.git
```

UNIX based Operating Systems
----------------------------

Before installing **pgquarrel**, you should have PostgreSQL 9.0+ installed (including the header files). If PostgreSQL is not in your search path add -DCMAKE_PREFIX_PATH=/path/to/pgsql to the cmake command.

```
$ tar -zxf pgquarrel-0.3.0.tgz
$ cd pgquarrel-0.3.0
$ cmake .
$ make
```

Windows
-------

You should have CMake 2.8.11+ installed and MS Visual Studio (tested with 2017). Open CMake Gui. If PostgreSQL is not in your path add an entry CMAKE_PREFIX_PATH (e.g. C:/Program Files/PostgreSQL/10). Change CMAKE_INSTALL_PREFIX if you want to install in another directory. Click on 'Configure' and then 'Generate'. Open MS Visual Studio project (path is specified in CMake Gui), right-click on ALL_BUILD and 'Compile'. After that right-click on INSTALL and 'Deploy'.

Features
========

**pgquarrel** does not support all of the PostgreSQL objects. Also, **pgquarrel** does not manipulate data (i.e. DML).

<table>
	<tr>
		<th>Object</th>
		<th>Support</th>
		<th>Comments</th>
	</tr>
	<tr>
		<td>AGGREGATE</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>CAST</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>COLLATION</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>COMMENT</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>CONVERSION</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>DOMAIN</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>EVENT TRIGGER</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>EXTENSION</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>FUNCTION</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>INDEX</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>LANGUAGE</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>MATERIALIZED VIEW</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>RULE</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>SCHEMA</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>SEQUENCE</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>TABLE</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>TRIGGER</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>TYPE</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>VIEW</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>GRANT</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>REVOKE</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>SECURITY LABEL</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>FOREIGN DATA WRAPPER</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>FOREIGN TABLE</td>
		<td>not implemented</td>
		<td></td>
	</tr>
	<tr>
		<td>SERVER</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>USER MAPPING</td>
		<td>complete</td>
		<td></td>
	</tr>
	<tr>
		<td>TEXT SEARCH CONFIGURATION</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>TEXT SEARCH DICTIONARY</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>TEXT SEARCH PARSER</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>TEXT SEARCH TEMPLATE</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>OPERATOR</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>OPERATOR CLASS</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>OPERATOR FAMILY</td>
		<td>partial</td>
		<td></td>
	</tr>
	<tr>
		<td>PUBLICATION</td>
		<td>not implemented</td>
		<td></td>
	</tr>
	<tr>
		<td>SUBSCRIPTION</td>
		<td>not implemented</td>
		<td></td>
	</tr>
	<tr>
		<td>ALTER DEFAULT PRIVILEGES</td>
		<td>uncertain</td>
		<td></td>
	</tr>
	<tr>
		<td>ALTER LARGE OBJECT</td>
		<td>uncertain</td>
		<td></td>
	</tr>
	<tr>
		<td>STATISTICS</td>
		<td>uncertain</td>
		<td></td>
	</tr>
</table>

Although **pgquarrel** does not support all PostgreSQL objects, it covers many of the use cases. In future releases, we expect to implement the TODO items to cover more cases. The main absences are:

* inheritance;
* roles.

Tests
=====

```
$ # adjust test/run-test.sh
$ cd pgquarrel-0.3.0/test
$ # test using 10 on both clusters
$ ./run-test.sh 10 10 init
```

License
=======

> Copyright © 2015-2018 Euler Taveira de Oliveira
> All rights reserved.

> Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

> Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer;
> Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution;
> Neither the name of the Euler Taveira de Oliveira nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

> THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

