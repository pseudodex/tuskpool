# Design principles

Rules for how code in this project is designed, tested and committed.
Each rule is written to be checkable. If you cannot tell whether code obeys a rule, the rule is too vague.

## Functions
- One function, one reason to change (SRP). If describing it needs "and", split it.
- Aim for at most 20 lines, 3 positional parameters, and low branching (cyclomatic complexity <= 8).
  Past 3 parameters, group them into a typed object.
- No boolean flag parameters that switch behaviour. Split into two functions or use an enum.
- Prefer pure functions: data in, data out, no hidden state. Keep I/O (files, network, DB,
  console, clock, randomness) at the edges and pass the results in.
- Name functions for what they do (verb) and values for what they are (noun). No `data`, `info`, `tmp`, `manager`.
- Replace magic numbers and strings with named constants or enums.
- Early return over deep nesting (guard clauses). Maximum nesting depth: 3.
- No commented-out code, no dead code, no TODOs without an owner or issue reference.

## Classes and types
- One class, one responsibility. Do not mix data, business rules, and persistence.
- Model data with typed, immutable-by-default structures. Never pass raw dicts or untyped objects across module boundaries.
- Prefer composition over inheritance. Inherit only for a true "is-a" relationship, and never more than one level deep.
- Depend on abstractions (protocols/interfaces) at module boundaries, concrete types inside a module.
- Inject dependencies (constructor or parameter). No hidden globals or singletons, and no constructing collaborators inside methods.
- Make invalid states unrepresentable: enums over strings, validated types over primitives where it prevents bugs.

## Modules and packages
- Related functions live in one module named for its purpose (`pricing`, not `utils`).
- Related modules live in one package that exposes a small public API (`__init__.py` / `index.ts`). Everything else is private.
- Dependencies point one way (see Layers). No import cycles.
- No `utils`, `helpers`, `common`, `misc` modules. If something has no home, the design is missing a concept.
- A module over ~300 lines or a package with unrelated modules should be split.

## Layers
```
interface (UI / CLI / API / jobs)   ->  services  ->  domain
```
- `domain`: entities, value objects, pure business rules. Imports nothing from other layers, no I/O.
- `services`: use cases that orchestrate domain objects and talk to the outside world through protocols/interfaces.
- `adapters` (db, http clients, file system): implement those protocols. Import `domain`, never the reverse.
- `interface`: parses input, calls ONE service, renders the result. Contains no business logic.

## Errors
- Fail loudly and early. Validate at the boundary, trust validated data inside.
- Define a small project exception hierarchy (`AppError` -> specific errors). Never raise or catch bare `Exception` except at the outermost boundary.
- Never swallow errors silently. Catch only what you can handle, and add context when re-raising.
- Translate errors to user-facing messages/status codes only in the interface layer.

## Patterns
- Use a design pattern only when it removes real duplication or coupling today. No speculative Factory/Strategy/Observer.
- A function or callable often replaces a single-method class (Strategy, Command, Factory).
- Rule of three: abstract on the third repetition, not the first.
- YAGNI: do not build for hypothetical future requirements.

## Configuration, logging, security
- Configuration comes from the environment or a typed settings object, loaded once at startup and passed in. No scattered `os.environ` / `process.env` reads.
- Never commit secrets. Never log secrets or personal data.
- Use a logger, not print/console.log, outside of CLI output meant for the user.
- Validate and sanitise all external input. Parameterise queries. Never build SQL, shell commands, or HTML by string concatenation.

## Testing
- Test behaviour through the public API. Do not test private functions directly.
- Domain and services get fast unit tests with no network, DB, or UI. Adapters get a few integration tests.
- One behaviour per test, named `test_<unit>_<condition>_<expected>`. Arrange / Act / Assert.
- Coverage is fixed at 100% (line and branch). Never lower the threshold, add ignore pragmas, or widen exclusions to get green: cover the code with a test or delete it. The only exclusions are bootstrap shims with no logic.
- Fix a bug by first writing a test that fails because of it.
- Fakes and stubs over mocks where practical. Inject them, do not patch internals.

## Documentation
- Type hints and PEP8 names are the documentation. Write a docstring or comment only where the code needs one: a quirk, a workaround, a non-obvious constraint, or brittle code that cannot reasonably be refactored. Say why, never what.
- No docstrings on ordinary modules, classes, functions or methods, and no comments that restate the code. If a comment explains what, rename or extract instead.
- Keep README commands copy-pasteable and tested.

## Version control
- Commit all your work, one commit per related change: a behaviour change together with its tests and docs. Not one commit per file or edit, and not one bulk commit at the end.
- Commit only when the check command passes, so every commit builds and tests green.
- Messages follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/#specification): `<type>[optional scope][!]: <description>`, then an optional body and footers. Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`. Use an imperative, lower-case description with no trailing period and a header under 72 characters. Mark breaking changes with `!` or a `BREAKING CHANGE:` footer. Use the body to explain why.
- The repository owner is the only author. No `Co-authored-by` trailers and no generated-by lines in messages.
- Never commit secrets, `.env` files, local databases or build output.
- Do not rewrite history that has been pushed, and do not push unless asked. The one exception is adopting the template in an existing project: its history is rewritten to these rules (step "History" in `MIGRATION.md`), on a backup first and force-pushed only after the user approves.
