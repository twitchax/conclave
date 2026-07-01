# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). The file is maintained with
[git-cliff](https://git-cliff.org/) (`cargo make changelog`).

## [Unreleased]

### Added

- **M0 — project scaffolding & hygiene.** Single-package `conclavelib` + `conclave` skeleton with
  the DESIGN §13 module SOC; the Cargo `[lints]` table, `rustfmt.toml`, and release/dev-release/
  profiling profiles; a `cargo-make` task graph with the canonical `ci = [fmt-check, clippy, test]`
  gate; a three-tier (unit / integration / e2e-spawns-binary) test harness on nextest + coverage;
  CI (lint / test / codecov / platform builds) and the Copilot setup workflow; and the docs and
  release scaffolding (README, CHANGELOG + git-cliff, DEVELOPMENT, CLAUDE.md, MIT LICENSE).
