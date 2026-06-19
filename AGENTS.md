# Agent instructions

## Use codebase-memory-mcp first

Before broad file-by-file exploration, use `codebase-memory-mcp` to query the project graph.

Start each new coding task by checking whether this repository is indexed:

1. Call the MCP tool `list_projects`.
2. If this project is missing or stale, call `index_repository` with:
   - `repo_path`: `/home/corbin/Development/tunnelAI`
3. Use graph queries before grep-style exploration:
   - `get_architecture` for the high-level layout.
   - `search_graph` to find functions, structs, modules, and tests by name.
   - `trace_path` to inspect callers and callees before changing behavior.
   - `detect_changes` after edits to understand the blast radius.
   - `get_code_snippet` for focused reads when you already know the symbol.

If the MCP tools are unavailable in the current session, say that and fall back to normal repository tools. Do not pretend the graph was queried.

## Local fallback commands

If only the CLI is available, use these from the repository root:

```bash
codebase-memory-mcp cli list_projects
codebase-memory-mcp cli index_repository '{"repo_path":"/home/corbin/Development/tunnelAI"}'
codebase-memory-mcp cli get_architecture '{"repo_path":"/home/corbin/Development/tunnelAI"}'
```

Use `search_graph` before text search when you need symbol-level context:

```bash
codebase-memory-mcp cli search_graph '{"name_pattern":".*Proxy.*"}'
```

## Development workflow

Read the relevant source before editing. Trace a symbol to its definition, tests, and call sites. Keep changes narrow.
Run the smallest useful check first, then the full relevant check before claiming the task is complete.

## Repository conventions
Interview the me relentlessly about every aspect of this plan until we reach a shared understanding. Walk down each branch of the design tree, resolving dependencies between decisions one-by-one. For each question, provide your recommended answer.

Ask the questions up to 5 at a time.
If a question can be answered by exploring the codebase/docs, explore the codebase/docs instead.
If the request is clear enough, do not ask questions for formality. Proceed directly.

## 2. Development Rules

- **All code changes must follow TDD cycles.** Write or update tests before or alongside implementation.
- **Always search for existing third-party crates before writing utilities from scratch.** If a well-maintained library already solves the problem, use it.
- **Code changes should be general instead of task/dataset specific.**
- **Breaking changes are allowed.** Prefer clean module boundaries over backwards compatibility.
