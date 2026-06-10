#pragma once

#include <cassert>

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
#include <cstdio>
#include <cstdlib>
#include <windows.h>

namespace akkara::test {
    [[noreturn]] inline void fail_fast_exit(const char* kind, const char* message = nullptr) noexcept {
        std::fprintf(stderr, "\n[%s]\n%s\n", kind ? kind : "MSVC TEST ERROR", message ? message : "(null)");
        std::fflush(stderr);
        std::_Exit(1);
    }

    [[noreturn]] inline void assertion_failed(const char* expression, const char* file, int line) noexcept {
        std::fprintf(
            stderr,
            "\n[TEST ASSERT]\n%s\n%s:%d\n",
            expression ? expression : "(null)",
            file ? file : "(unknown)",
            line
        );
        std::fflush(stderr);
        std::_Exit(1);
    }

    inline void __cdecl invalid_parameter_handler(
        const wchar_t*,
        const wchar_t*,
        const wchar_t*,
        unsigned int,
        uintptr_t
    ) {
        fail_fast_exit("MSVC INVALID PARAMETER");
    }

    inline void __cdecl purecall_handler() {
        fail_fast_exit("MSVC PURE VIRTUAL CALL");
    }

    inline void signal_handler(int signal) {
        fail_fast_exit(signal == SIGABRT ? "MSVC SIGABRT" : "MSVC SIGNAL");
    }

    static int __cdecl crt_report_hook(int report_type, char* message, int* return_value) {
        const char* kind = "CRT";

        switch (report_type) {
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

        if (return_value) { *return_value = 1; }
        if (report_type == _CRT_ERROR || report_type == _CRT_ASSERT) { fail_fast_exit(kind, message); }
        return TRUE;
    }

    inline void install_msvc_test_error_handlers() {
        SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);
        _set_error_mode(_OUT_TO_STDERR);
        _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
        _set_invalid_parameter_handler(invalid_parameter_handler);
        _set_purecall_handler(purecall_handler);
        std::set_terminate([] { fail_fast_exit("MSVC TERMINATE"); });
        std::signal(SIGABRT, signal_handler);
        _CrtSetReportHook(crt_report_hook);
    }

} // namespace akkara::test

#undef assert
#define assert(expression) \
    ((static_cast<bool>(expression)) ? static_cast<void>(0) : ::akkara::test::assertion_failed(#expression, __FILE__, __LINE__))

#else

namespace akkara::test {
    inline void install_msvc_test_error_handlers() noexcept {}
}

#endif
