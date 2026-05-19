# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Apache Flink Agents — an event-driven, streaming Agentic AI framework built on Apache Flink. Provides parallel **Java and Python** APIs for building agents that run as Flink jobs (or in a local environment), with first-class abstractions for chat models, prompts, tools, memory, MCP, vector stores, and dynamic event-driven orchestration.

## Build, test, lint

All commands assume repo root unless noted. Scripts in `tools/` wrap the canonical workflows — prefer them over invoking `mvn`/`pytest` directly.

```shell
# Full build (Java multi-module + Python wheel; Python install pulls the dist jars)
./tools/build.sh
./tools/build.sh -j        # Java only
./tools/build.sh -p        # Python only (requires Java already built so dist jars exist)

# Tests
./tools/ut.sh              # Java + Python, default Flink version
./tools/ut.sh -j           # Java only
./tools/ut.sh -p           # Python only
./tools/ut.sh -f 1.20      # Run against a specific Flink version (supported: 2.2, 1.20)
./tools/ut.sh -e           # E2E suite (separate target — not part of default ut.sh run)

# Lint / format (Python ruff + Java spotless)
./tools/lint.sh            # apply formatting (default)
./tools/lint.sh -c         # check only (CI mode)

# License headers
./tools/check-license.sh
```

Single-test invocation:
- Java: `mvn -pl <module> -Dtest=<ClassName>#<method> test` (e.g. `-pl runtime`). Requires `mvn install -DskipTests` first to publish sibling test-jars to the local repo (this is what `ut.sh` does).
- Python: from `python/`, `uv run --no-sync pytest flink_agents/<path>/tests/test_x.py::TestClass::test_method`. The `-k "not e2e_tests"` filter is what skips e2e in normal runs.

`SKIP_SPOTLESS_CHECK=true` is the standard escape hatch when iterating — CI's dedicated style job owns enforcement, so test/build jobs set this to avoid masking real failures with formatting violations. Run `./tools/lint.sh` before pushing.

## Repository layout (multi-module Maven + co-located Python package)

The Java project is a Maven multi-module build rooted at `pom.xml`; the Python package lives in `python/` and is wired so that `tools/build.sh` copies the built Java dist jars into `python/flink_agents/lib/` before producing the wheel. Java and Python ship together — you cannot meaningfully test the Python side without the Java jars present.

Top-level Maven modules:

- **`api/`** — Public Java API: `Agent`, `AgentBuilder`, events, annotations (`@Action`, `@ChatModelSetup`, `@Tool`, etc.), resource abstractions (chat/embedding models, vector stores, MCP, memory, prompts). This is the dependency boundary for users embedding Flink Agents.
- **`plan/`** — Compiles agent definitions into a serializable, language-agnostic `AgentPlan` (action graph + resource providers). The plan is what crosses the Java↔Python boundary.
- **`runtime/`** — Executes plans on Flink. Contains the operators, async actions, event log, action-state, resource cache, Python bridge (Pemja-based), and feedback queues. Code here is what actually runs inside a Flink TaskManager.
- **`integrations/`** — Optional connector modules grouped by kind: `chat-models/{anthropic,azureai,bedrock,ollama,openai}`, `embedding-models/{bedrock,ollama}`, `vector-stores/{elasticsearch,milvus,opensearch,s3vectors}`, `mcp/`. Each is its own Maven module — add new providers here.
- **`dist/`** — Builds the shipped jar bundles. Critically: a single shared `common/` jar (heavy deps, ~110MB) plus per-Flink-version "thin" jars under `flink-1.20`, `flink-2.0`, `flink-2.1`, `flink-2.2`. Cross-version compatibility is handled by selecting the right thin jar at runtime/install time, not by shading versioned classes into one fat jar. When touching code that depends on Flink internals, verify it compiles cleanly across all `flink-*` dist modules.
- **`e2e-test/`** — Three separate suites: agent-plan compatibility (cross-version plan deserialization), integration (full pipeline), and resource-cross-language (Java agents using Python resources and vice versa). Excluded from the default `ut.sh` run; gated behind `-e`.
- **`examples/`**, **`ide-support/`** — Sample agents and IDE wiring.

Python package mirror (`python/flink_agents/`):

- **`api/`** — Python equivalent of Java `api/`: decorators, `ExecutionEnvironment`, chat/embedding/vector-store/memory/tools/prompts/yaml entrypoints.
- **`plan/`** — Python plan builder. Plans built here are JSON-serializable into the same format as Java plans.
- **`runtime/`** — Two execution modes: `local_*` (in-process, no Flink) and Flink-backed (`flink_runner_context`, `flink_memory_object`, `flink_metric_group`, plus `java/` Pemja bridge code). `remote_execution_environment.py` submits to a real Flink cluster.
- **`integrations/`** — Python integrations parallel to the Java ones.
- **`examples/`**, **`e2e_tests/`**, **`tests/`**.

## Architectural notes that affect how you work

**Dual-language, single plan.** The same agent can be authored in Java or Python, but at runtime it is compiled into an `AgentPlan` (defined in `plan/`) which is shared between languages. When changing event types, action signatures, resource providers, or any plan-shape construct, you must update **both** the Java side (`api/`, `plan/`, `runtime/`) and the Python side (`flink_agents/api/`, `flink_agents/plan/`, `flink_agents/runtime/`) to keep the wire format compatible. The cross-language e2e suite (`e2e-test/flink-agents-end-to-end-tests-resource-cross-language/`) is what catches drift here — run it when touching plan/runtime types.

**Multi-Flink-version support is a build-time concern, not runtime.** `dist/flink-*` modules each compile against a different Flink version. New code in `runtime/` that touches Flink APIs must compile under the lowest supported version (1.20 currently); use `dist/`'s `common/` for version-agnostic shared code.

**Event-driven orchestration.** Agents are structured as `@Action` handlers reacting to typed events (see `api/.../event/`: `ChatRequestEvent`, `ToolRequestEvent`, etc.). The `runtime/` event log is the source of truth for replay and observability — don't add side-channel state when an event would do.

**Python-in-Java via Pemja.** When Python actions or resources run inside a Java-driven Flink job, the bridge lives in `runtime/.../python/` (Java) and `python/flink_agents/runtime/java/` (Python). Object marshalling here is the most common source of cross-language bugs.

## Conventions worth knowing

- Java target is 11; spotless uses `googleJavaFormat 1.15.0` AOSP style with import order `,javax,java,scala,\#`. JDK 21 builds skip spotless (plugin doesn't support it).
- Python is 3.10–3.12, managed with `uv` (pinned to `0.11.0`); ruff config is strict (annotations, docstrings via Google convention, etc.) — see `python/pyproject.toml`. Relative imports are banned (`flake8-tidy-imports: ban-relative-imports = "all"`).
- PR titles must be prefixed with components, e.g. `[api]`, `[runtime]`, `[python]`, `[hotfix]` — see `.github/CONTRIBUTING.md`.
