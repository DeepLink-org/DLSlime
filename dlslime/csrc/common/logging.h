//
// Created by NanoDeploy Team
//

#pragma once

#include <cxxabi.h>
#include <execinfo.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <source_location>
#include <string>

namespace nanocommon {

// -----------------------------------------------------------------------------
// Environment Variables
// -----------------------------------------------------------------------------

inline std::string get_env_variable(char const* env_var_name)
{
    if (!env_var_name) {
        return "";
    }
    char* lvl = getenv(env_var_name);
    if (lvl)
        return std::string(lvl);
    return "";
}

inline int& get_log_level_internal()
{
    static int level = []() {
        std::string lvl_str = get_env_variable("NANOCOMMON_LOG_LEVEL");
        if (lvl_str.empty())
            return 1;  // Default to INFO (1). 0=ERROR, 1=INFO, 2=DEBUG
        return std::stoi(lvl_str);
    }();
    return level;
}

inline int get_log_level()
{
    return get_log_level_internal();
}

inline void set_log_level(int level)
{
    get_log_level_internal() = level;
}

inline bool is_mutex_logging_enabled()
{
    static bool enabled = []() {
        std::string val = get_env_variable("NANOCOMMON_LOG_MUTEX");
        return !val.empty() && std::stoi(val) != 0;
    }();
    return enabled;
}

inline std::mutex& get_console_mutex()
{
    static std::mutex mtx;
    return mtx;
}

// -----------------------------------------------------------------------------
// Stack Trace Utility
// -----------------------------------------------------------------------------

inline void print_stack_trace()
{
    const int max_frames = 64;
    void*     addr_list[max_frames];

    int addr_len = backtrace(addr_list, max_frames);

    if (addr_len == 0) {
        std::cerr << "  <empty, possibly corrupt stack>" << std::endl;
        return;
    }

    char** symbol_list = backtrace_symbols(addr_list, addr_len);
    if (!symbol_list) {
        std::cerr << "  <failed to resolve stack symbols>" << std::endl;
        return;
    }

    std::unique_ptr<char*, void (*)(void*)> symbol_guard(symbol_list, [](void* p) { free(p); });

    std::cerr << "\033[1;96m"
              << "\n--- Stack Trace ---\n"
              << "\033[m";

    for (int i = 0; i < addr_len; ++i) {
        std::string symbol = symbol_list[i];
        std::string func_name;
        std::string offset;
        std::string address;

        size_t open_paren  = symbol.find('(');
        size_t plus_sign   = symbol.find('+', open_paren);
        size_t close_paren = symbol.find(')', plus_sign);

        if (open_paren != std::string::npos && plus_sign != std::string::npos && close_paren != std::string::npos) {
            std::string mangled = symbol.substr(open_paren + 1, plus_sign - open_paren - 1);
            offset              = symbol.substr(plus_sign, close_paren - plus_sign);
            address             = symbol.substr(close_paren + 1);

            int                                    status = -1;
            std::unique_ptr<char, void (*)(void*)> demangled(
                abi::__cxa_demangle(mangled.c_str(), nullptr, nullptr, &status), [](void* p) { std::free(p); });

            if (status == 0 && demangled) {
                func_name = demangled.get();
            }
            else {
                func_name = mangled;
            }
        }
        else {
            func_name = symbol;
        }

        std::cerr << "#" << i << " " << func_name << " "
                  << "\033[90m" << offset << " " << address << "\033[m" << std::endl;
    }
    std::cerr << std::endl;
}

// -----------------------------------------------------------------------------
// Locking and Output Macros (Internal Helpers)
// -----------------------------------------------------------------------------

#define NANOCOMMON_CONSOLE_LOCK                                                                                        \
    std::unique_lock<std::mutex> _nano_console_lock(nanocommon::get_console_mutex(), std::defer_lock);                 \
    if (nanocommon::is_mutex_logging_enabled()) {                                                                      \
        _nano_console_lock.lock();                                                                                     \
    }

// -----------------------------------------------------------------------------
// Logging Implementation with C++20 source_location
// -----------------------------------------------------------------------------

template<typename... Args>
void log_message(int level, const char* level_str, const char* color, const std::source_location& loc, Args&&... args)
{
    if (get_log_level() >= level) {
        NANOCOMMON_CONSOLE_LOCK
        std::cerr << color << "[" << level_str << "]"
                  << "\033[m " << loc.file_name() << ":" << loc.line() << ": " << loc.function_name() << ": ";
        ((std::cerr << std::forward<Args>(args)), ...);
        std::cerr << std::endl;
    }
}

// Variadic template for assertions to support custom messages with stream
// operators
template<typename... Args>
void assertion_failed(const char* expr_str, const std::source_location& loc, Args&&... args)
{
    {
        NANOCOMMON_CONSOLE_LOCK
        std::cerr << "\033[1;91m"
                  << "[Assertion Failed]"
                  << "\033[m " << loc.file_name() << ":" << loc.line() << ": " << loc.function_name()
                  << ", Expected: " << expr_str << ". Error msg: ";
        ((std::cerr << std::forward<Args>(args)), ...);
        std::cerr << std::endl;
    }
    print_stack_trace();
    abort();
}

template<typename... Args>
void abort_with_message(const std::source_location& loc, Args&&... args)
{
    {
        NANOCOMMON_CONSOLE_LOCK
        std::cerr << "\033[1;91m"
                  << "[Fatal]"
                  << "\033[m " << loc.file_name() << ":" << loc.line() << ": " << loc.function_name() << ": ";
        ((std::cerr << std::forward<Args>(args)), ...);
        std::cerr << std::endl;
    }
    print_stack_trace();
    abort();
}

// -----------------------------------------------------------------------------
// Public Logging Macros
// -----------------------------------------------------------------------------

// Helper struct to capture source location implicitly
struct LogLoc {
    std::source_location loc;
    LogLoc(std::source_location l = std::source_location::current()): loc(l) {}
};

template<typename... Args>
void log_error(LogLoc loc, Args&&... args)
{
    log_message(0, "ERROR", "\033[1;91m", loc.loc, std::forward<Args>(args)...);
}

template<typename... Args>
void log_warn(LogLoc loc, Args&&... args)
{
    log_message(1, "WARN", "\033[1;93m", loc.loc, std::forward<Args>(args)...);
}

template<typename... Args>
void log_info(LogLoc loc, Args&&... args)
{
    log_message(1, "INFO", "\033[1;92m", loc.loc, std::forward<Args>(args)...);
}

template<typename... Args>
void log_debug(LogLoc loc, Args&&... args)
{
    log_message(2, "DEBUG", "\033[1;94m", loc.loc, std::forward<Args>(args)...);
}

#define NANOCOMMON_LOG_ERROR(...)                                                                                      \
    nanocommon::log_message(0, "ERROR", "\033[1;91m", std::source_location::current(), __VA_ARGS__)
#define NANOCOMMON_LOG_WARN(...)                                                                                       \
    nanocommon::log_message(1, "WARN", "\033[1;93m", std::source_location::current(), __VA_ARGS__)
#define NANOCOMMON_LOG_INFO(...)                                                                                       \
    nanocommon::log_message(1, "INFO", "\033[1;92m", std::source_location::current(), __VA_ARGS__)
#define NANOCOMMON_LOG_DEBUG(...)                                                                                      \
    nanocommon::log_message(2, "DEBUG", "\033[1;94m", std::source_location::current(), __VA_ARGS__)

#define NANOCOMMON_ASSERT(Expr, ...)                                                                                   \
    if (!(Expr)) {                                                                                                     \
        nanocommon::assertion_failed(#Expr, std::source_location::current() __VA_OPT__(, ) __VA_ARGS__);               \
    }

#define NANOCOMMON_ASSERT_EQ(A, B, ...) NANOCOMMON_ASSERT((A) == (B), __VA_ARGS__)
#define NANOCOMMON_ASSERT_NE(A, B, ...) NANOCOMMON_ASSERT((A) != (B), __VA_ARGS__)
#define NANOCOMMON_ASSERT_GT(A, B, ...) NANOCOMMON_ASSERT((A) > (B), __VA_ARGS__)
#define NANOCOMMON_ASSERT_GE(A, B, ...) NANOCOMMON_ASSERT((A) >= (B), __VA_ARGS__)
#define NANOCOMMON_ASSERT_LT(A, B, ...) NANOCOMMON_ASSERT((A) < (B), __VA_ARGS__)
#define NANOCOMMON_ASSERT_LE(A, B, ...) NANOCOMMON_ASSERT((A) <= (B), __VA_ARGS__)

#define NANOCOMMON_ABORT(...) nanocommon::abort_with_message(std::source_location::current(), __VA_ARGS__)

}  // namespace nanocommon
