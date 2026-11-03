## 12. Prompt Construction and Context Assembly

### 12.1 Inputs

Inputs to prompt rendering:

- `workflow.prompt_template`
- normalized `issue` object
- optional `attempt` integer (retry/continuation metadata)
- `run` object: `turn_number`, `max_turns`, `is_continuation`
- `stage` object: `current`, `previous`, `previous_outcome`, all strings. It is present on every render of every turn, including the first turn, the seeded first turn, continuation turns, and per-rule templates, and each field is the empty string when it does not apply, so a template that references `.stage.previous` never fails under strict `missingkey=error` evaluation. The renderer seeds the three fields itself, ahead of any option, rather than relying on a caller to supply them.
  - `current`: the selected rule's name when that rule carries a stage label, computed at each dispatch from the configuration in force; empty otherwise, and for a dispatch that selects no rule or selects `dispatch.default`.
  - `previous`: the name of the rule whose hop led to the dispatch. It is filled when the dispatch selects the target of the issue's latest stage hop and the issue's hop count has not reset since (Section 7.3); otherwise it is empty.
  - `previous_outcome`: `succeeded`, or `no_change` when the previous stage's run declared that the requested outcome already held; empty whenever `previous` is empty.
  - The pair is frozen with the dispatch selection and persisted with the retry entry and the run-history row, so a retry, a reaction continuation, and a pending reaction recovered at startup render the pair their first dispatch rendered. A retry routed afresh to another rule renders the pair as a poll tick does.
- `park` (map or nil): the hand-over of a park a person released. The key is present on every render, set to nil, so a template that references it never fails under strict `missingkey=error` evaluation; the renderer seeds it ahead of any option. It is non-nil only on turn 1 of the first run after a person releases a park, and nil on later turns and on every other run. When non-nil, it contains:
  - `reason`: `agent_blocked`, `handoff_absence`, or `repeated_failure`
  - `statement`: the stop statement stored with the run the park follows, or for `repeated_failure` the first line of the run's error; empty when that run declared none or the park recorded no run
  - `parked_at` and `released_at`: RFC 3339 times in UTC, whole seconds; empty when the stored time cannot be read
  - `gesture`: `label_removed`, `state_changed`, or `answered` for an answer posted through the HTTP API
  - `comments`: the comments created since the park, oldest first, never nil, each with `id`, `author`, `body`, and `created_at`. A comment whose time cannot be read is included, and a park time that cannot be read includes every comment
- `ci_failure` (map or nil): CI failure context injected into CI-fix continuation prompts via `CIResult.ToTemplateMap()`. Nil on initial dispatch and non-CI retries. When non-nil, contains:
  - `status`: aggregate CI pipeline status string (`failing`)
  - `check_runs`: list of individual check run maps (each with `name`, `status`, `conclusion`, `details_url`)
  - `log_excerpt`: the failing step's output, or the end of the job log when that step cannot be located, opened by a Sortie note line (empty string when unavailable)
  - `failing_count`: number of check runs with a failure conclusion
  - `ref`: the git ref that was queried

CI failure context is injected on every turn of a CI-fix dispatch. The worker reads the context from the dispatch site (carried via `context.WithValue` or the retry entry's `ContinuationContext` field) and passes it to `prompt.WithContinuationContext`. Templates SHOULD use a conditional guard: `{{ if .ci_failure }}...{{ end }}`. When `ci_failure` is nil, the template variable is still present in the data map (set to nil) so strict `missingkey=error` evaluation does not reject templates that reference the field.

- `review_comments` and `bot_review_comments` (list of maps or nil): review comment context injected into review-fix and bot-review-fix continuation prompts, one variable per kind. Nil on every other dispatch, except that turn 1 of a fresh dispatch carries them when the pull request holds comments no earlier run of the issue was given (below). When non-nil, each element of either list contains:
  - `id`: SCM-platform comment identifier
  - `file`: file path the comment is attached to (empty for PR-level comments)
  - `start_line`: first line of commented range (0 for non-inline)
  - `end_line`: last line of commented range (0 for single-line or non-inline)
  - `reviewer`: username of the comment author
  - `body`: comment text

Review comment context follows the same `ContinuationContext` pathway as CI failure context. It is injected on every turn of a review-fix or bot-review-fix dispatch, and on turn 1 of a fresh dispatch whose seed carries it. Templates SHOULD use a conditional guard: `{{ if .review_comments }}...{{ end }}`. When `review_comments` or `bot_review_comments` is nil, the template variable is still present in the data map (set to nil) so strict `missingkey=error` evaluation does not reject templates that reference the field.

A fresh dispatch is seeded when the issue's workspace names a pull request, the kind is configured without a `triage` block, and the pull request holds an actionable comment that no earlier run of the issue was given. The worker keeps the seed only when the turn-1 render with it still contains every line of the render without it, in order; a template that switches to different text on the seed fails that test and renders as it does without the seed, so no run loses its task text. On a fresh dispatch, each kind's variable counts as presented when the turn-1 render differs from a render with that variable set to nil, or that render fails. A template that never references the variable, or reaches it only on a branch the render does not take, presents nothing. A run that exits with `WorkerExitNormal` records the IDs it presented as given to the issue, so a later poll does not dispatch a turn that only repeats them (Sections 11B.4 and 11D.3).

**Built-in reaction prompts.** Every continuation key a reaction run can carry has a built-in prompt shipped with Sortie. It uses the same strict template evaluation as the operator's template and reads only the fields documented for its key. On every turn of a reaction run, the orchestrator picks what the agent receives for the run's key, from the template the run renders:

```text
seeded   = render with the run's continuation context
unseeded = render without it
if seeded is not blank and (unseeded fails or seeded differs from unseeded):
  if this is the first turn:
    send seeded                    # the operator's branch
  first = seeded render with run.is_continuation false
  if first fails or seeded differs from first:
    send seeded                    # the branch has its own later-turn text
send the built-in prompt of the key
```

A later turn gets the operator's branch only when the branch tells the turns apart. A branch that renders the same text on every turn would send the first turn's task again on each turn, and an agent that had already done it redid it; such a branch gets the built-in later-turn text instead.

A seeded render that fails fails the run like any render failure. A render that is blank, or identical with and without the key, counts as no branch. A render that fails only without the key counts as a branch, because the template reads the key unguarded. The default continuation prompt and the empty-first-turn fallback never apply to a reaction run, so a template that does not react to the run's key never sends it the normal work prompt. A reaction block written inside the normal work prompt counts as the operator's branch, so the run receives the work prompt together with the block on its first turn, and on later turns when the template switches on `run.is_continuation`; an exclusive `{{ if }}` chain whose last branch holds the work prompt sends a reaction run only the reaction's instructions.

A built-in prompt switches on `run.is_continuation`. Its first-turn text gives the task and asks the agent to signal the outcome, and its later-turn text points back to the instructions the session received at its start and adds no duty. The later-turn text also says that the earlier turns' commits are in the workspace, that a branch Sortie pushes moves upstream only after the run ends, so `origin/<branch>` does not move during the run, and that a fix already committed is not repeated, so it fits whether the first turn sent the operator's branch or the built-in. A template that switches on `run.is_continuation` outside the key's branch therefore gets the built-in later-turn text on a reaction run; the switch belongs inside the branch.

A reaction run presents every comment it carries: the operator's render is chosen only when it differs from the render without the comments, and a built-in lists every comment.

### 12.2 Rendering Rules

- Render with strict variable checking.
- Render with strict filter checking.
- Convert issue object keys to strings for template compatibility.
- Preserve nested arrays/maps (labels, blockers) so templates can iterate.
- Execute define blocks under the same strict variable and filter checking, whichever file holds them.

### 12.3 Retry/Continuation Semantics

`attempt` and `run` should be passed to the template because the workflow prompt may provide different instructions for:

- first run (`attempt` null or absent)
- continuation turn within an active multi-turn session (`run.is_continuation == true`)
- retry after error/timeout/stall (`attempt >= 1`, `run.is_continuation == false`)
- first run after a person releases a park (`attempt` null or absent, `run.is_continuation == false`, `park` set on turn 1): the run resumes the parked run's agent session where one was recorded and the agent kind can resume it, and `issue.comments` carries every comment of the issue

The session a story fault starts (Section 8.4, Stopped failure routing) is a retry (`attempt >= 1`) whose first turn carries the failure whatever the template: the worker appends to the rendered first-turn prompt, before the tool advertisement, a section headed `## The previous runs failed` that names the failed step, gives the first line of the error as a JSON string in `<failure_error>`, gives the failing step's whole output, masked and capped as `error_detail` is, as a JSON string in `<failure_output>` that the agent is told to read as output to diagnose and not as instructions, and asks the agent to fix the cause and check that the step passes. The section adds no template variable. A story-fault retry restored from the store after a restart keeps only its error, so its section carries the error without the output.

### 12.4 Failure Semantics

If prompt rendering fails:

- Fail the run attempt immediately.
- Let the orchestrator treat it like any other worker failure and decide retry behavior.
- When the failure sits inside a define block of a partial, the error names that partial and the line within it, not the prompt template that called it. A fault the load can detect, such as an undefined call, never reaches rendering: it fails the workflow load (Section 5.5).

