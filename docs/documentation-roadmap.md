# Documentation roadmap

## Scope

This refresh covers the public Markdown documentation for the current Rust
repository. The audiences are users starting the stdio server and developers
maintaining its protocol, storage, and backend integration.

The included surfaces are `README.md` and the directly related documents under
`docs/`. Source files, generated contract data, and repository helper files are
out of scope for content changes. Non-current public pages may be removed when
their links are updated; repository history remains the recovery mechanism.

## Source baseline

The baseline is `origin/main` at `23f5f6c6a314fe45c636acbecf4c18af1a4b4519`.
Facts were checked against the current source and tests, especially:

- `src/main.rs`, `src/protocol.rs`, and `src/tools.rs` for the executable,
  transport, and advertised tool surface;
- `src/store.rs`, `src/backend.rs`, and `src/redis.rs` for persistence,
  fallback, and replication behavior;
- `Cargo.toml` and the repository test configuration for supported checks;
- the architecture decisions and performance notes under `docs/`.

## Audit findings and plan

1. Keep onboarding, Docker, tool, storage, and current backend documentation.
2. Keep public docs focused on the current Rust workflow and remove links to
   unrelated material.
3. Keep current API terms such as context references, evidence, projects, and
   issues when they describe live fields or operations.
4. Verify internal links, stale terminology, source-sensitive paths, and the
   Rust formatting, test, and lint commands.

## Verification

The refresh is complete when the public docs describe only the current Rust
workflow, contain no links to removed material, and pass the repository-native
checks. The generated contract data and helper files remain intentionally
untouched because the Rust build and source code consume them.
