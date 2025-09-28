# Repository Guidelines

## Project Structure & Module Organization
- Core Spark comparison logic lives in `src/main/scala` (`DiffGenerator.scala`, `DuplicateDetector.scala`, `SummaryGenerator.scala`, `Main.scala`).
- Shared fixtures and ScalaTest suites sit in `src/test/scala`; always reuse `SparkSessionTestWrapper` for Spark-aware specs.
- Helper assets reside under `scripts/` (bootstrap, PowerShell variants) and top-level runners like `run_compare.sh` and `tests_run.sh` orchestrate common flows.
- Generated artefacts land in `spark-warehouse/` (Parquet diffs, duplicates, summary) and `output/` (Excel reports); keep them out of commits.
- SBT metadata is in `project/`, while `build.sbt` defines the single-module assembly build on Scala 2.12.

## Build, Test, and Development Commands
- `./scripts/bootstrap.sh` (or `./scripts/bootstrap.ps1`) installs JDK 11/sbt via Coursier and prepares the local Hadoop stub.
- `sbt compile` validates source changes; pair with IntelliJ’s Metals or sbt console for faster iteration.
- `./run_compare.sh` launches the local reference comparison and writes results to `spark-warehouse/` for inspection.
- `sbt test` or `./tests_run.sh` runs the ScalaTest suite with a controlled `JAVA_HOME`; the wrapper also clears lingering sbt daemons.
- `sbt assembly` produces `target/scala-2.12/compare-assembly.jar`, the thin JAR expected by downstream jobs and CI deploy steps.

## Coding Style & Naming Conventions
- Follow the existing two-space indentation inside blocks; keep braces on the same line as declarations and align multiline parameter lists.
- Use `PascalCase` for classes/objects (`CompareConfig`), `camelCase` for methods and vals (`generateDifferencesTable`), and UPPER_SNAKE_CASE only for true constants.
- Prefer immutable vals, explicit return types on public APIs, and `Option`/`Seq` over nulls; keep log messages/comments bilingual (Spanish/English) to match the codebase.
- Validate Spark schemas with `StructType` literals and normalise strings via helpers such as `DiffGenerator.canonicalize` instead of ad-hoc logic.

## Testing Guidelines
- Create suites in `src/test/scala` named `*Spec.scala`, extending `AnyFlatSpec` with `Matchers` and `SparkSessionTestWrapper` to reuse the shared session.
- Cover happy-path and edge scenarios (nulls, duplicated keys, partition specs); prefer DataFrame-based assertions when comparing outputs.
- Run `sbt test` before every push; incorporate `./tests_run.sh` in pipelines so CI mirrors local environment settings.

## Commit & Pull Request Guidelines
- Match existing history: short, present-tense summaries (often Spanish) such as `ajusto métricas de summary`; keep under ~70 characters.
- Provide a focused description, link the relevant Jira/GitHub issue, and attach outputs when they aid review (e.g., new Parquet samples or `summary.xlsx`).
- Confirm `sbt test` (and `sbt assembly` when altering packaging) in the PR body; flag any schema or KPI shifts that require downstream consumers to adapt.
