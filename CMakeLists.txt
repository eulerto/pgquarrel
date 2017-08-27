cmake_minimum_required(VERSION 2.8.11)

project(pgquarrel C)
add_executable(pgquarrel
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

add_subdirectory(mini)

if(CMAKE_C_COMPILER_ID STREQUAL "GNU" OR CMAKE_C_COMPILER_ID MATCHES "Clang")
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -fno-strict-aliasing -fwrapv -g")
endif()

# cmake -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_PREFIX_PATH=/home/euler/pg962 .
find_package(PostgreSQL REQUIRED)
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
else()
	message(FATAL_ERROR "could not find PostgreSQL")
endif(PostgreSQL_FOUND)

set(LIBS ${LIBS} ${PostgreSQL_LIBRARIES})
# CMake does not include libpgport by default for Windows
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
	set(LIBS ${LIBS} ${PostgreSQL_LIBRARY_DIRS}/libpgport.lib)
endif()

include_directories(mini)

set(LIBS ${LIBS} mini)

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
	--indent=force-tab=4
	--indent-switches
	--pad-oper
	--align-pointer=name
	--remove-brackets
	--max-code-length=80
	--break-after-logical
#	--suffix=none
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
