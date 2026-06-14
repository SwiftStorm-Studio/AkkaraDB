#pragma once

#if defined(_WIN32) && !defined(AKKARADB_STATIC)
#  if defined(AKKARADB_CLUSTER_RUNTIME_BUILD_SHARED)
#    define AKKARADB_CLUSTER_RUNTIME_API __declspec(dllexport)
#  else
#    define AKKARADB_CLUSTER_RUNTIME_API __declspec(dllimport)
#  endif
#else
#  define AKKARADB_CLUSTER_RUNTIME_API
#endif
