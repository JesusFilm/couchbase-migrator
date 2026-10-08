# Couchbase SDK 3.2.7 native build

Modern C++ standard library headers no longer provide fixed-width integer types
through unrelated includes. The vendored libcouchbase code uses `std::uint8_t`,
`std::uint16_t` and `std::uint32_t` without including `<cstdint>`.

The patch adds that standard include to its common public header for C++ callers
and its standalone utilities header. C callers retain their existing includes.
It changes no SDK API, versions, compiler options or runtime behavior.

`pnpm build` ensures the real native SDK loads after an install that skipped
lifecycle scripts. The packaged CLI smoke tests import both operational modules
without invoking database connections, ingestion or migration commands.
