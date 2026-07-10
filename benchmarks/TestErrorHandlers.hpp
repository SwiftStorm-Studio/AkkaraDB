/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/TestErrorHandlers.hpp
#pragma once

#include <cstdio>
#include <cstdlib>

namespace akkaradb::test {
    [[noreturn]] inline void failFastExit(const char* kind, const char* message = nullptr) noexcept {
        std::fprintf(stderr, "\n[%s]\n%s\n", kind ? kind : "TEST ERROR", message ? message : "(null)");
        std::fflush(stderr);
        std::_Exit(1);
    }

    [[noreturn]] inline void assertionFailed(const char* expression, const char* file, int line) noexcept {
        std::fprintf(
            stderr,
            "\n[TEST CHECK]\n%s\n%s:%d\n",
            expression ? expression : "(null)",
            file ? file : "(unknown)",
            line
        );
        std::fflush(stderr);
        std::_Exit(1);
    }
} // namespace akkaradb::test

#if defined(_MSC_VER) && defined(_DEBUG)

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <crtdbg.h>
#include <csignal>
#include <exception>
#include <windows.h>

namespace akkaradb::test {
    inline void __cdecl invalidParameterHandler(
        const wchar_t*,
        const wchar_t*,
        const wchar_t*,
        unsigned int,
        uintptr_t
    ) {
        failFastExit("MSVC INVALID PARAMETER");
    }

    inline void __cdecl purecallHandler() {
        failFastExit("MSVC PURE VIRTUAL CALL");
    }

    inline void signalHandler(int signal) {
        failFastExit(signal == SIGABRT ? "MSVC SIGABRT" : "MSVC SIGNAL");
    }

    static int __cdecl crtReportHook(int reportType, char* message, int* returnValue) {
        const char* kind = "CRT";

        switch (reportType) {
            case _CRT_WARN:
                kind = "CRT WARN";
                break;
            case _CRT_ERROR:
                kind = "CRT ERROR";
                break;
            case _CRT_ASSERT:
                kind = "CRT ASSERT";
                break;
            default:
                break;
        }

        std::fprintf(stderr, "\n[%s]\n%s\n", kind, message ? message : "(null)");

        if (returnValue) { *returnValue = 1; }
        if (reportType == _CRT_ERROR || reportType == _CRT_ASSERT) { failFastExit(kind, message); }
        return TRUE;
    }

    inline void routeCrtReportsToStderr() {
        _CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_FILE);
        _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_FILE);
        _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_FILE);
        _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDERR);
        _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDERR);
        _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDERR);
    }

    inline void installMsvcTestErrorHandlers() {
        SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);
        _set_error_mode(_OUT_TO_STDERR);
        _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
        routeCrtReportsToStderr();
        _set_invalid_parameter_handler(invalidParameterHandler);
        _set_purecall_handler(purecallHandler);
        std::set_terminate([] { failFastExit("MSVC TERMINATE"); });
        std::signal(SIGABRT, signalHandler);
        _CrtSetReportHook(crtReportHook);
    }

} // namespace akkaradb::test

#else

namespace akkaradb::test {
    inline void installMsvcTestErrorHandlers() noexcept {}
}

#endif

#define AKK_TEST_CHECK(expression) \
    ((static_cast<bool>(expression)) ? static_cast<void>(0) : ::akkaradb::test::assertionFailed(#expression, __FILE__, __LINE__))
