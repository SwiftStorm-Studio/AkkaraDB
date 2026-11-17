/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
// Run separately from throughput measurement. This samples only the selected benchmark thread.
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <dbghelp.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {
    struct Handle {
        HANDLE value;
        ~Handle() { if (value) { CloseHandle(value); } }
    };
    std::string quote(std::string text) {
        size_t pos = 0;
        while ((pos = text.find('"', pos)) != std::string::npos) { text.insert(pos++, 1, '"'); ++pos; }
        return '"' + text + '"';
    }
    struct Symbols {
        HANDLE process;
        ~Symbols() { SymCleanup(process); }
        std::string resolve(DWORD64 address) {
            alignas(SYMBOL_INFO) char buffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME]{};
            auto* symbol = reinterpret_cast<SYMBOL_INFO*>(buffer);
            symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
            symbol->MaxNameLen = MAX_SYM_NAME;
            DWORD64 displacement = 0;
            const bool found = SymFromAddr(process, address, &displacement, symbol) != FALSE;
            IMAGEHLP_MODULE64 module{};
            module.SizeOfStruct = sizeof(module);
            const bool moduleFound = SymGetModuleInfo64(process, address, &module) != FALSE;
            return std::string{moduleFound ? module.ModuleName : "unknown"} + '!' +
                (found ? std::string{symbol->Name} : "0x" + std::to_string(address));
        }
    };
}

int main(int argc, char** argv) {
    try {
        const bool activeWindow = argc == 7 && std::string{argv[6]} == "--active-window";
        if (argc != 6 && !activeWindow) {
            std::cerr << "Usage: windows_cpu_sampler PID TID CONTROL_PREFIX OUTPUT_DIR SYMBOL_DIR [--active-window]\n";
            return 2;
        }
        const DWORD pid = std::stoul(argv[1]), tid = std::stoul(argv[2]);
        const std::filesystem::path control = argv[3], output = argv[4];
        std::filesystem::create_directories(output);
        const Handle process{OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ | SYNCHRONIZE, FALSE, pid)};
        const Handle thread{OpenThread(THREAD_GET_CONTEXT | THREAD_SUSPEND_RESUME | THREAD_QUERY_INFORMATION, FALSE, tid)};
        if (!process.value || !thread.value || GetProcessIdOfThread(thread.value) != pid) {
            throw std::runtime_error("could not open the selected process/thread");
        }
        const auto eventName = L"Local\\AkkStorageProfile-" + std::to_wstring(pid) + L"-" + std::to_wstring(tid);
        const Handle started{activeWindow ? OpenEventW(SYNCHRONIZE, FALSE, (eventName + L"-started").c_str()) : nullptr};
        const Handle finished{activeWindow ? OpenEventW(SYNCHRONIZE, FALSE, (eventName + L"-finished").c_str()) : nullptr};
        if (activeWindow && (!started.value || !finished.value)) { throw std::runtime_error("profile window events are unavailable"); }
        // sleep_for(1ms) can round up to the system timer period. Prefer a
        // high-resolution waitable timer without changing process-wide timer policy.
        HANDLE timerHandle = CreateWaitableTimerExW(nullptr, nullptr, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
        const bool highResolutionTimer = timerHandle != nullptr;
        if (!timerHandle) { timerHandle = CreateWaitableTimerW(nullptr, FALSE, nullptr); }
        const Handle timer{timerHandle};
        if (!timer.value) { throw std::runtime_error("could not create sampling timer"); }
        SymSetOptions(SYMOPT_UNDNAME | SYMOPT_DEFERRED_LOADS | SYMOPT_LOAD_LINES | SYMOPT_FAIL_CRITICAL_ERRORS | SYMOPT_NO_PROMPTS);
        if (!SymInitialize(process.value, argv[5], TRUE)) { throw std::runtime_error("SymInitialize failed"); }
        Symbols symbols{process.value};
        // Resolve and load PDBs before releasing the benchmark, avoiding first-sample symbol loading while paused.
        SymEnumerateModules64(process.value, [](PCSTR, DWORD64 base, PVOID context) -> BOOL {
            auto* owner = static_cast<Symbols*>(context);
            (void)owner->resolve(base + 0x1000);
            return TRUE;
        }, &symbols);
        std::vector<std::vector<DWORD64>> stacks;
        stacks.reserve(100'000);
        uint64_t failures = 0;
        uint64_t pauseNs = 0;
        std::ofstream go{control.string() + ".go"};
        go.exceptions(std::ios::badbit | std::ios::failbit);
        go << "attached\n";
        go.close();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(5);
        const auto stopped = [&] {
            return WaitForSingleObject(process.value, 0) != WAIT_TIMEOUT || (activeWindow
                                                                                 ? WaitForSingleObject(finished.value, 0) == WAIT_OBJECT_0
                                                                                 : std::filesystem::exists(control.string() + ".done"));
        };
        while (activeWindow && WaitForSingleObject(started.value, 0) != WAIT_OBJECT_0 && !stopped()) {
            if (std::chrono::steady_clock::now() > deadline) { throw std::runtime_error("profile window did not start"); }
            (void)WaitForSingleObject(started.value, 10);
        }
        while (!stopped()) {
            if (std::chrono::steady_clock::now() > deadline) { throw std::runtime_error("sampling exceeded five minutes"); }
            LARGE_INTEGER due{};
            due.QuadPart = -10'000; // Relative 1ms, in 100ns units.
            if (!SetWaitableTimer(timer.value, &due, 0, nullptr, nullptr, FALSE) || WaitForSingleObject(timer.value, 1000) != WAIT_OBJECT_0) {
                throw std::runtime_error("sampling timer failed");
            }
            if (stopped()) { break; }
            std::vector<DWORD64> stack;
            stack.reserve(48);
            const auto paused = std::chrono::steady_clock::now();
            if (SuspendThread(thread.value) == DWORD(-1)) { ++failures; continue; }
            CONTEXT context{};
            context.ContextFlags = CONTEXT_FULL;
            if (GetThreadContext(thread.value, &context)) {
                STACKFRAME64 frame{};
                frame.AddrPC.Offset = context.Rip;
                frame.AddrPC.Mode = AddrModeFlat;
                frame.AddrStack.Offset = context.Rsp;
                frame.AddrStack.Mode = AddrModeFlat;
                frame.AddrFrame.Offset = context.Rbp;
                frame.AddrFrame.Mode = AddrModeFlat;
                for (size_t depth = 0; depth < 48 && frame.AddrPC.Offset; ++depth) {
                    stack.push_back(frame.AddrPC.Offset);
                    if (!StackWalk64(IMAGE_FILE_MACHINE_AMD64, process.value, thread.value, &frame, &context,
                        nullptr, SymFunctionTableAccess64, SymGetModuleBase64, nullptr)) { break; }
                    if (stack.size() > 1 && frame.AddrPC.Offset == stack.back()) { break; }
                }
            }
            const bool resumed = ResumeThread(thread.value) != DWORD(-1);
            pauseNs += static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - paused).count());
            if (!resumed) { throw std::runtime_error("ResumeThread failed"); }
            if (stack.empty()) { ++failures; }
            else { stacks.push_back(std::move(stack)); }
        }
        // Retain raw addresses as well as symbolized inclusive/exclusive counts.
        std::ofstream raw{output / "stacks.csv"}, report{output / "functions.csv"}, metadata{output / "sampling.txt"};
        raw.exceptions(std::ios::badbit | std::ios::failbit);
        report.exceptions(std::ios::badbit | std::ios::failbit);
        metadata.exceptions(std::ios::badbit | std::ios::failbit);
        raw << "sample,depth,address,symbol\n";
        std::map<DWORD64, std::string> names;
        std::map<std::string, std::pair<uint64_t, uint64_t>> counts;
        for (size_t i = 0; i < stacks.size(); ++i) {
            std::set<std::string> inclusive;
            for (size_t depth = 0; depth < stacks[i].size(); ++depth) {
                const auto address = stacks[i][depth];
                auto [it, inserted] = names.try_emplace(address);
                if (inserted) { it->second = symbols.resolve(address); }
                const auto& name = it->second;
                raw << i << ',' << depth << ',' << address << ',' << quote(name) << '\n';
                if (depth == 0) { ++counts[name].first; }
                inclusive.insert(name);
            }
            for (const auto& name : inclusive) { ++counts[name].second; }
        }
        std::vector<std::pair<std::string, std::pair<uint64_t, uint64_t>>> sorted{counts.begin(), counts.end()};
        std::ranges::sort(sorted, [](const auto& a, const auto& b) { return a.second.first > b.second.first; });
        report << "symbol,exclusive_samples,inclusive_samples,exclusive_percent,inclusive_percent\n";
        for (const auto& [name, count] : sorted) {
            report << quote(name) << ',' << count.first << ',' << count.second << ','
                << 100.0 * count.first / stacks.size() << ',' << 100.0 * count.second / stacks.size() << '\n';
        }
        metadata << "method=external SuspendThread/GetThreadContext/StackWalk64 wall-clock stack sampling\n"
            << "pid=" << pid << "\ntid=" << tid << "\nsamples=" << stacks.size() << "\nfailed_samples=" << failures << "\nactive_window_events=" << activeWindow
            << "\nhigh_resolution_timer=" << highResolutionTimer << "\ntarget_pause_ns=" << pauseNs << "\nrequested_sleep_ms=1\n"
            << "This is an intrusive diagnostic run, not a throughput measurement.\n"
            << "Samples can include off-CPU waits; percentages are wall-clock observations, not hardware CPU counters.\n"
            << "Optimized/inlined functions can be attributed to their enclosing symbol.\n";
        raw.close(); report.close(); metadata.close();
        if (stacks.empty()) { throw std::runtime_error("no stacks collected"); }
        std::cout << "Collected " << stacks.size() << " stacks; failures=" << failures << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Sampler failed: " << error.what() << " (Windows error " << GetLastError() << ")\n";
        return 2;
    }
}
