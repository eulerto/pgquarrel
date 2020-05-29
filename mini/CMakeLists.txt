if(APPLE)
	set( CMAKE_MACOSX_RPATH 1 )
endif()

add_library(mini SHARED
	mini-file.c
	mini-file.h
	mini-parser.c
	mini-parser.h
	mini-readline.c
	mini-readline.h
	mini-strip.c
	mini-strip.h
)

install(TARGETS mini DESTINATION lib)
