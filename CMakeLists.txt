cmake_minimum_required(VERSION 2.8.11)

project(pgquarrel C)
add_executable(pgquarrel
	src/am.c
	src/am.h
	src/aggregate.c
	src/aggregate.h
	src/cast.c
	src/cast.h
	src/collation.c
	src/collation.h
	src/common.c
	src/common.h
	src/conversion.c
	src/conversion.h
	src/domain.c
	src/domain.h
	src/eventtrigger.c
	src/eventtrigger.h
	src/extension.c
	src/extension.h
	src/fdw.c
	src/fdw.h
	src/function.c
	src/function.h
	src/index.c
	src/index.h
	src/language.c
	src/language.h
	src/matview.c
	src/matview.h
	src/operator.c
	src/operator.h
	src/policy.c
	src/policy.h
	src/privileges.c
	src/privileges.h
	src/quarrel.c
	src/quarrel.h
	src/rule.c
	src/rule.h
	src/schema.c
	src/schema.h
	src/sequence.c
	src/sequence.h
	src/server.c
	src/server.h
	src/statistics.c
	src/statistics.h
	src/table.c
	src/table.h
	src/textsearch.c
	src/textsearch.h
	src/trigger.c
	src/trigger.h
	src/type.c
	src/type.h
	src/usermapping.c
	src/usermapping.h
	src/view.c
	src/view.h
)

# ignore Windows warnings
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
	add_definitions(/wd4005 /D_CRT_SECURE_NO_WARNINGS /D_CRT_SECURE_NO_DEPRECATE /D_CRT_NONSTDC_NO_DEPRECATE)
endif()

add_subdirectory(mini)

if(CMAKE_C_COMPILER_ID STREQUAL "GNU" OR CMAKE_C_COMPILER_ID MATCHES "Clang")
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -fno-strict-aliasing -fwrapv -g")
endif()

# cmake -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_PREFIX_PATH=/home/euler/pg101 .
find_package(PostgreSQL QUIET)

# check pg_config
find_program(PGCONFIG_PATH NAMES pg_config)
if(PGCONFIG_PATH)
	message("pg_config: ${PGCONFIG_PATH}")
else()
	message(FATAL_ERROR "could not find pg_config")
endif()

if(PostgreSQL_FOUND)
	include_directories(${PostgreSQL_INCLUDE_DIRS})
	# some postgres headers (those included by optionals) aren't on the include
	# path. Hence, we need to include them.
	if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
		include_directories(/usr/local/include)
	endif()
	# some Windows include paths
	if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
		include_directories(${PostgreSQL_INCLUDE_DIR}/server/port/win32 ${PostgreSQL_INCLUDE_DIR}/server/port/win32_msvc)
	endif()
	# include static library (for prompt password)
	# unfortunately CMake does not include it
	execute_process(COMMAND ${PGCONFIG_PATH} --libdir RESULT_VARIABLE result OUTPUT_VARIABLE pgpath OUTPUT_STRIP_TRAILING_WHITESPACE)
	if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
		set(LIBS "${pgpath}/libpgport.lib")
	else()
		set(LIBS "${pgpath}/libpgport.a")
	endif()
else()
	# setup library directory
	execute_process(COMMAND ${PGCONFIG_PATH} --libdir RESULT_VARIABLE result OUTPUT_VARIABLE pgpath OUTPUT_STRIP_TRAILING_WHITESPACE)
	set(PostgreSQL_LIBRARY_DIRS "${pgpath}")

	# figure out library directory and try to find libpgport
	if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
#		execute_process(COMMAND ${PGCONFIG_PATH} --libdir RESULT_VARIABLE result OUTPUT_VARIABLE pgpath OUTPUT_STRIP_TRAILING_WHITESPACE)
		if(EXISTS "${pgpath}/libpgport.lib")
			set(LIBS "${pgpath}/libpgport.lib")
		else()
			message(FATAL_ERROR "could not find libpgport.lib")
		endif()
	else()
#		execute_process(COMMAND ${PGCONFIG_PATH} --libdir RESULT_VARIABLE result OUTPUT_VARIABLE pgpath OUTPUT_STRIP_TRAILING_WHITESPACE)
		if(EXISTS "${pgpath}/libpgport.a")
			set(LIBS "${pgpath}/libpgport.a")
		else()
			# Debian-like systems move static libraries to PKGLIBDIR. Let's probe it too.
			execute_process(COMMAND ${PGCONFIG_PATH} --pkglibdir OUTPUT_VARIABLE pgpath OUTPUT_STRIP_TRAILING_WHITESPACE)
			if(EXISTS "${pgpath}/libpgport.a")
				set(LIBS "${pgpath}/libpgport.a")
			else()
				message(FATAL_ERROR "could not find libpgport.a")
			endif()
		endif()
	endif()

	# setup include directories
	execute_process(COMMAND ${PGCONFIG_PATH} --includedir OUTPUT_VARIABLE pgpath OUTPUT_STRIP_TRAILING_WHITESPACE)
	set(PostgreSQL_INCLUDE_DIRS ${pgpath})
	execute_process(COMMAND ${PGCONFIG_PATH} --includedir-server OUTPUT_VARIABLE pgpath OUTPUT_STRIP_TRAILING_WHITESPACE)
	set(PostgreSQL_INCLUDE_DIRS ${PostgreSQL_INCLUDE_DIRS} ${pgpath})

	# setup libpq for linking into pgquarrel
	set(PostgreSQL_LIBRARIES "pq")
	include_directories(${PostgreSQL_INCLUDE_DIRS})
endif(PostgreSQL_FOUND)

message("PostgreSQL FOUND: ${PostgreSQL_FOUND}")
message("LIBS: ${LIBS}")
message("PostgreSQL LIBRARIES: ${PostgreSQL_LIBRARIES}")
message("PostgreSQL LIBRARY DIRS: ${PostgreSQL_LIBRARY_DIRS}")
message("PostgreSQL INCLUDE DIRS: ${PostgreSQL_INCLUDE_DIRS}")

set(LIBS ${LIBS} ${PostgreSQL_LIBRARIES})

include_directories(mini)

set(LIBS ${LIBS} mini)

# postgres libraries are not installed in one of the standard system directories
# or
# postgres library directory is not in the dynamic linker
#set_target_properties(pgquarrel PROPERTIES LINK_FLAGS "-L${PostgreSQL_LIBRARY_DIRS}")

target_link_libraries(pgquarrel ${LIBS})

install(TARGETS pgquarrel RUNTIME DESTINATION bin)
# copy DLLs into bin directory for Windows
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
	install(FILES ${PostgreSQL_LIBRARY_DIRS}/libpq.dll DESTINATION bin)
	install(FILES ${PROJECT_BINARY_DIR}/mini/$<CONFIGURATION>/mini.dll DESTINATION bin)
endif()

find_program(astyle_EXECUTABLE astyle DOC "source code indenter, formatter, and beautifier")

add_custom_target("style" COMMAND
	"${astyle_EXECUTABLE}"
	--style=bsd
#	--attach-closing-while
	--indent=force-tab=4
	--indent-switches
	--pad-oper
	--align-pointer=name
	--align-reference=name
	--remove-brackets
#	--break-return-type
#	--break-return-type-decl
	--max-code-length=80
	--break-after-logical
	--suffix=none
	--lineend=linux
	${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
	${CMAKE_CURRENT_SOURCE_DIR}/src/*.h
	VERBATIM
)

# coverage
#include(CodeCoverage)
#set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O0 -fprofile-arcs -ftest-coverage")
#setup_target_for_coverage(coverage_target mytests coverage_dir)

# cmake -DCMAKE_BUILD_TYPE=Debug ..
# make
# make coverage_target

#
# MANUAL COVERAGE
#
# uncomment the line below:
#set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
#
# $ cd pgquarrel/
# $ rm -rf CMakeFiles/
# $ cmake -DCMAKE_PREFIX_PATH=/home/euler/pg945 . ; make clean; make
# $ cd test
# $ ./run-test.sh init
# point your browser to pgquarrel/coverage/index.html
