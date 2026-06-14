#pragma once

#if defined(_WIN32) && !defined(AKKARADB_STATIC)
#  if defined(AKKARADB_API_SERVER_BUILD_SHARED)
#    define AKKARADB_API_SERVER_API __declspec(dllexport)
#  else
#    define AKKARADB_API_SERVER_API __declspec(dllimport)
#  endif
#else
#  define AKKARADB_API_SERVER_API
#endif
