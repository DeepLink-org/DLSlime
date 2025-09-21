//
// Created by jimy on 3/13/22.
//

#pragma once

#include "env.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

namespace slime {
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

#define STREAM_VAR_ARGS1(a) << a
#define STREAM_VAR_ARGS2(a, b) << a << b
#define STREAM_VAR_ARGS3(a, b, c) << a << b << c
#define STREAM_VAR_ARGS4(a, b, c, d) << a << b << c << d
#define STREAM_VAR_ARGS5(a, b, c, d, e) << a << b << c << d << e
#define STREAM_VAR_ARGS6(a, b, c, d, e, f) << a << b << c << d << e << f
#define STREAM_VAR_ARGS7(a, b, c, d, e, f, g) << a << b << c << d << e << f << g
#define STREAM_VAR_ARGS8(a, b, c, d, e, f, g, h) << a << b << c << d << e << f << g << h

#define GET_MACRO(_1, _2, _3, _4, _5, _6, _7, _8, NAME, ...) NAME

#define STREAM_VAR_ARGS(...)                                                                                           \
    GET_MACRO(__VA_ARGS__,                                                                                             \
              STREAM_VAR_ARGS8,                                                                                        \
              STREAM_VAR_ARGS7,                                                                                        \
              STREAM_VAR_ARGS6,                                                                                        \
              STREAM_VAR_ARGS5,                                                                                        \
              STREAM_VAR_ARGS4,                                                                                        \
              STREAM_VAR_ARGS3,                                                                                        \
              STREAM_VAR_ARGS2,                                                                                        \
              STREAM_VAR_ARGS1)                                                                                        \
    (__VA_ARGS__)

#define SLIME_ASSERT(Expr, Msg, ...)                                                                                   \
    {                                                                                                                  \
        if (!(Expr)) {                                                                                                 \
            std::cerr << "\033[1;91m"                                                                                  \
                      << "[Assertion Failed]"                                                                          \
                      << "\033[m " << __FILE__ << ":" << __LINE__ << ": " << __FUNCTION__ << ", Expected: " << #Expr   \
                      << ". Error msg: " << Msg __VA_OPT__(STREAM_VAR_ARGS(__VA_ARGS__)) << std::endl;                 \
            abort();                                                                                                   \
        }                                                                                                              \
    }

#define SLIME_ASSERT_EQ(A, B, Msg, ...) SLIME_ASSERT((A) == (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_NE(A, B, Msg, ...) SLIME_ASSERT((A) != (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_GT(A, B, Msg, ...) SLIME_ASSERT((A) > (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_GE(A, B, Msg, ...) SLIME_ASSERT((A) >= (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_LT(A, B, Msg, ...) SLIME_ASSERT((A) < (B), Msg, __VA_ARGS__)
#define SLIME_ASSERT_LE(A, B, Msg, ...) SLIME_ASSERT((A) <= (B), Msg, __VA_ARGS__)

#define SLIME_ABORT(Msg, ...)                                                                                          \
    {                                                                                                                  \
        std::cerr << "\033[1;91m"                                                                                      \
                  << "[Fatal]"                                                                                         \
                  << "\033[m " << __FILE__ << ":" << __LINE__ << ": " << __FUNCTION__ << ": "                          \
                  << Msg __VA_OPT__(STREAM_VAR_ARGS(__VA_ARGS__)) << std::endl;                                        \
        abort();                                                                                                       \
    }

#define SLIME_LOG_LEVEL(MsgType, FlagFormat, Level, ...)                                                               \
    {                                                                                                                  \
        if (get_log_level() >= Level) {                                                                                \
            std::cerr << FlagFormat << "[" << MsgType << "]"                                                           \
                      << "\033[m " << __FILE__ << ":" << __LINE__ << ": " << __FUNCTION__                              \
                      << ": " __VA_OPT__(STREAM_VAR_ARGS(__VA_ARGS__)) << std::endl;                                   \
        }                                                                                                              \
    }

// Error and Warn
#define SLIME_LOG_ERROR(...) SLIME_LOG_LEVEL("ERROR", "\033[1;91m", 0, __VA_ARGS__)
#define SLIME_LOG_WARN(...) SLIME_LOG_LEVEL("WARN", "\033[1;91m", 1, __VA_ARGS__)

// Info
#define SLIME_LOG_INFO(...) SLIME_LOG_LEVEL("INFO", "\033[1;92m", 1, __VA_ARGS__)

// Debug
#define SLIME_LOG_DEBUG(...) SLIME_LOG_LEVEL("DEBUG", "\033[1;92m", 2, __VA_ARGS__)
}  // namespace slime
