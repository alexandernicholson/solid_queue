---
name: assess-and-refactor
description: Review recent project changes holistically, write a prioritized lore plan, get approval for every refactor opportunity, then implement approved changes and rerun the project tests. Use when asked to assess and auto-refactor recent work.
---

# Assess and refactor recent changes

The approval gate is mandatory: **do not implement any changes without user approval**, including Quick items. Assessment produces a plan; implementation begins after plan approval and explicit decisions for each opportunity.

## Writing style

Apply this to every user-facing message and every written artifact, including lore plans, `TODO.md`, code documentation, **CLAUDE.md**, and **.cursor/rules/**: **Do not talk about what we don't do, talk about what we have. Clarity and brevity without being opaque.** State the existing behavior, the proposed or completed change, its value, and concrete evidence. Keep operational approval and verification requirements explicit.

## Phase 1: Assess

1. Enter plan mode when the host provides `EnterPlanMode`. In OMP, use assessment mode: inspect and write the plan while implementation remains gated on approval.
2. Review recent changes in the thread and repository holistically. Inspect the relevant diff and surrounding code; preserve existing user changes. Identify substantive opportunities involving duplication, inconsistent or bloated implementation, unused or obsolete code, meaningful missing coverage, and architectural improvements. Consolidate shared logic with restraint.
3. Consider whether **CLAUDE.md** needs conventions for Claude to follow or **.cursor/rules/** needs workflow, syntax, or caveat guidance. Include a documentation opportunity when useful; assessment records proposed edits in the lore plan. Write in the upstream project's style and follow the Writing style rule for the plan and any future docs.
4. Create `lore/YYYYMMDD-HHMM-description.md` using the local date and time. Include a numbered **Effort/Impact Assessment Table** with *every* identified opportunity, sorted Quick first, then Easy, Moderate, Hard:

   | # | Opportunity | Effort | Impact | Action |
   |---|-------------|--------|--------|--------|
   | 1 | Description | Quick/Easy/Moderate/Hard | Low/Medium/High | Auto-fix / Ask first / Skip |

   - **Quick:** under 5 minutes, mechanical and zero risk; e.g. identical logic consolidated, inconsistent naming unified, unused methods/constants or dead paths removed.
   - **Easy:** 5–15 minutes, straightforward and minimal risk.
   - **Moderate:** 15–60 minutes, requires thought and has some risk.
   - **Hard:** over an hour, architectural and higher risk.
   - **High impact:** bugs, performance, or significant maintainability gains. **Medium:** readability, duplication, or better patterns. **Low:** minor style or cleanup.
   - `Action` is a recommendation, **not authorization**. `Auto-fix` still requires explicit approval. Number items so the user can refer to them later.
5. Below the table add **Opportunity Details** for every item: **What** (functional change), **Where** (files/classes/methods), **Why** (value), and **Trade-offs** (risks, if any). Keep the plan actionable, specific, and consistent with the Writing style rule.
6. End the lore file with this exact section:

   ```markdown
   ## Execution Protocol
   **DO NOT implement any changes without user approval.**
   For EACH opportunity, use `AskUserQuestion`.
   Options: "Implement" / "Skip (add to TODO.md)" / "Do not implement"
   Ask ALL questions before beginning any implementation work.
   (do NOT do alternating ask then implement, ask then implement, etc.)
   Quick items may be batched into one multi-select AskUserQuestion.
   After all items resolved, run: `bin/rails test`
   ```
7. Exit plan mode with `ExitPlanMode` when available and present the plan for user approval. In OMP, present the lore path and request approval before execution. State what the plan contains and what decisions come next, following the Writing style rule. Plan approval precedes each item's disposition.

## Phase 2: Execute after plan approval

1. Read the lore plan to recover the complete opportunity list. Summarize the selected changes with a small diagram and a short paragraph, emphasizing stability, performance, and observability.
2. Create a task for **every** opportunity (`TaskCreate` in Claude Code; `todo` in OMP). Ask for the disposition of **every** opportunity before implementing any: **Implement**, **Skip (add to TODO.md)**, or **Do not implement**. Use `AskUserQuestion` in Claude Code and `ask` in OMP; batch related questions where supported. A Quick multi-select is acceptable only if the response unambiguously resolves every item to one of the three dispositions. Do not treat a recommendation or plan approval as an item decision.
3. For **Implement**, change the approved code; keep code comments focused on behavior rather than plan item numbers. For **Skip**, add an entry to `TODO.md` with the lore path (for example, `(lore/filename.md)`), phrased under the Writing style rule. For **Do not implement**, remove the item from the task list. Keep edits within the approved scope.
4. After triage, parallelize genuinely independent approved items by file ownership. In Claude Code, create an Agent Team with `TeamCreate` and spawn teammates using `Task` with `team_name`; prefer Sonnet unless the slice needs Opus. In OMP, spawn **named subagents** in one `task` call with `context` and `tasks[]` (one item per independent file-ownership slice). Their names are peer IDs in OMP's agent roster; use `hub list` to inspect peers and `hub send` to coordinate, and read each delivered result or `agent://<id>` output. Give each subagent its item description, exact owned files, the relevant test command, and its file-ownership boundary. Run builds, formatters, linters, and tests once after integration. Implement overlapping or dependent edits sequentially. Review all results and address feedback.
5. When all items are resolved, run `bin/rails test` from the project root (the Rails test command used by CI). Report the actual result and any environmental blockers; never claim a pass without observing it.
6. Tell the user what was implemented, what was put in `TODO.md`, what was declined, and the test result. Follow the Writing style rule: lead with the behavior and evidence.
