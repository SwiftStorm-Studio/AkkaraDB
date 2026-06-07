#pragma once

#if defined(_MSC_VER) && defined(_DEBUG)

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <crtdbg.h>
#include <cstdio>
#include <cstdlib>
#include <windows.h>

namespace akkara::test {

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
        return TRUE;
    }

    inline void install_msvc_test_error_handlers() {
        SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);
        _set_error_mode(_OUT_TO_STDERR);
        _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
        _CrtSetReportHook(crt_report_hook);
    }

} // namespace akkara::test

#else

namespace akkara::test {
    inline void install_msvc_test_error_handlers() noexcept {}
}

#endif
