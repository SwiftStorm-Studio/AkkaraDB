if(NOT DEFINED REVISION_FILE)
    message(FATAL_ERROR "REVISION_FILE is required")
endif()

if(EXISTS "${REVISION_FILE}")
    file(READ "${REVISION_FILE}" CURRENT_REVISION)
    string(STRIP "${CURRENT_REVISION}" CURRENT_REVISION)
else()
    set(CURRENT_REVISION "0")
endif()

if(NOT CURRENT_REVISION MATCHES "^[0-9]+$")
    message(FATAL_ERROR "Revision file must contain only an integer: ${REVISION_FILE}")
endif()

math(EXPR NEXT_REVISION "${CURRENT_REVISION} + 1")
file(WRITE "${REVISION_FILE}" "${NEXT_REVISION}\n")
message(STATUS "AkkaraDB revision incremented: r${CURRENT_REVISION} -> r${NEXT_REVISION}")
