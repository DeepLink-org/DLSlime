#pragma once

#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>

#include "dlslime/csrc/env.h"

namespace dlslime {

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

inline int get_log_level()
{
    return SLIME_LOG_LEVEL;
}

inline bool is_mutex_logging_enabled()
{
    return SLIME_LOG_MUTEX;
}

inline std::mutex& get_console_mutex()
{
    static std::mutex mtx;
    return mtx;
}

#define SLIME_CONSOLE_LOCK                                                                                             \
    std::unique_lock<std::mutex> _slime_console_lock(dlslime::get_console_mutex(), std::defer_lock);                   \
    if (dlslime::is_mutex_logging_enabled()) {                                                                         \
        _slime_console_lock.lock();                                                                                    \
    }

// LogProxy to handle mixed << and , syntax
struct LogProxy {
    std::ostream& os;
    explicit LogProxy(std::ostream& os): os(os) {}

    template<typename T>
    LogProxy& operator<<(const T& val)
    {
        os << val;
        return *this;
    }

    template<typename T>
    LogProxy& operator,(const T& val)
    {
        os << val;
        return *this;
    }

    // Handle stream manipulators (e.g. std::hex)
    typedef std::ostream& (*StreamManipulator)(std::ostream&);
    LogProxy& operator<<(StreamManipulator manip)
    {
        os << manip;
        return *this;
    }
    LogProxy& operator,(StreamManipulator manip)
    {
        os << manip;
        return *this;
    }
};

template<typename Func>
inline void log_impl(
    int level, const char* msg_type, const char* flag_format, const char* file, int line, const char* func, Func fn)
{
    if (get_log_level() >= level) {
        SLIME_CONSOLE_LOCK
        std::cerr << flag_format << "[" << msg_type << "]\033[m " << file << ":" << line << ": " << func << ": ";
        fn(std::cerr);
        std::cerr << std::endl;
    }
}

template<typename Func>
inline void abort_impl(const char* file, int line, const char* func, Func fn)
{
    {
        SLIME_CONSOLE_LOCK
        std::cerr << "\033[1;91m[Fatal]\033[m " << file << ":" << line << ": " << func << ": ";
        fn(std::cerr);
        std::cerr << std::endl;
    }
    abort();
}

template<typename Func>
inline void assert_impl(bool expr, const char* expr_str, const char* file, int line, const char* func, Func fn)
{
    if (!expr) {
        {
            SLIME_CONSOLE_LOCK
            std::cerr << "\033[1;91m[Assertion Failed]\033[m " << file << ":" << line << ": " << func
                      << ", Expected: " << expr_str << ". Error msg: ";
            fn(std::cerr);
            std::cerr << std::endl;
        }
        abort();
    }
}

#define SLIME_ASSERT(Expr, Msg, ...)                                                                                   \
    {                                                                                                                  \
        dlslime::assert_impl(                                                                                          \
            static_cast<bool>(Expr), #Expr, __FILE__, __LINE__, __FUNCTION__, [&](std::ostream& _slime_os) {           \
                dlslime::LogProxy(_slime_os) << Msg __VA_OPT__(, __VA_ARGS__);                                         \
            });                                                                                                        \
    }

#define SLIME_ASSERT_EQ(A, B, Msg, ...) SLIME_ASSERT((A) == (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_NE(A, B, Msg, ...) SLIME_ASSERT((A) != (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_GT(A, B, Msg, ...) SLIME_ASSERT((A) > (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_GE(A, B, Msg, ...) SLIME_ASSERT((A) >= (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_LT(A, B, Msg, ...) SLIME_ASSERT((A) < (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_LE(A, B, Msg, ...) SLIME_ASSERT((A) <= (B), Msg, __VA_ARGS__)

#define SLIME_ABORT(...)                                                                                               \
    {                                                                                                                  \
        dlslime::abort_impl(__FILE__, __LINE__, __FUNCTION__, [&](std::ostream& _slime_os) {                           \
            dlslime::LogProxy(_slime_os) << __VA_ARGS__;                                                               \
        });                                                                                                            \
    }

#define SLIME_LOG_LEVEL(MsgType, FlagFormat, Level, ...)                                                               \
    {                                                                                                                  \
        dlslime::log_impl(Level, MsgType, FlagFormat, __FILE__, __LINE__, __FUNCTION__, [&](std::ostream& _slime_os) { \
            dlslime::LogProxy(_slime_os) << __VA_ARGS__;                                                               \
        });                                                                                                            \
    }

// Error and Warn
#define SLIME_LOG_ERROR(...) SLIME_LOG_LEVEL("ERROR", "\033[1;91m", 0, __VA_ARGS__)
#define SLIME_LOG_WARN(...) SLIME_LOG_LEVEL("WARN", "\033[1;91m", 1, __VA_ARGS__)

// Info
#define SLIME_LOG_INFO(...) SLIME_LOG_LEVEL("INFO", "\033[1;92m", 1, __VA_ARGS__)

// Debug
#define SLIME_LOG_DEBUG(...) SLIME_LOG_LEVEL("DEBUG", "\033[1;92m", 2, __VA_ARGS__)
}  // namespace dlslime
