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

This patch fixes the observed C++ compilation error only. SDK 3.2.7 is not
listed as supported on Node 24 by Couchbase; vendor support starts at SDK 4.7.0.
A successful local native load does not establish server or migration behavior.
See the runtime review in the root README before accepting this combination.
