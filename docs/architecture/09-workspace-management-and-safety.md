## 9. Workspace Management and Safety

### 9.1 Workspace Layout

Workspace root:

- `workspace.root` (normalized path; the current config layer expands path-like values and preserves bare relative names)

Per-issue workspace path:

- `<workspace.root>/<sanitized_issue_identifier>`

Workspace persistence:

- Workspaces are reused across runs for the same issue.
- Successful runs do not auto-delete workspaces.

Workspace cleanup runs through the following mechanisms, ordered and non-overlapping in intent:

- **The terminal gate** is the primary mechanism. It is always on and unconditional: whenever the tracker reports an issue's state as a member of `tracker.terminal_states`, the workspace for that issue is removed, whether that gate fires from startup cleanup, active-run reconciliation, or the periodic sweep.
- **The age bound** is a backstop for the population the terminal gate can never reach: an issue parked in the workflow's handoff state with no external automation advancing it, an issue left active after a permanent failure, an issue moved to a state the configuration does not name, or an issue deleted from the tracker entirely. It is opt-in and off by default, configured by `workspace.retention_days`, and evaluated only by the periodic sweep. A workspace is removable by age when its latest recorded activity is older than the configured window. That activity is anchored on the later of two timestamps: the most recent `completed_at` recorded in the run history for the workspace's identifier, and the `pushed_at` value recorded in the workspace's SCM record (`.sortie/scm.json`, or `.sortie-host/scm.json` under `remote` with a repository, Section 9.7). A workspace with neither a parseable completion nor a parseable recorded push is retained regardless of how long it has sat on disk: absence of a record is absence of evidence, not evidence of age.

The age bound never fires on a workspace the terminal gate would have removed on the same pass, because the terminal check runs first. It applies only to the periodic sweep, not to startup cleanup or active-run reconciliation; see the polling and reconciliation material for why.

### 9.2 Workspace Creation and Reuse

Input: `issue.identifier`

Algorithm summary:

1. Sanitize identifier to `workspace_key`.
2. Compute workspace path under workspace root.
3. Ensure the workspace path exists as a directory.
4. Mark `created_now=true` only if the directory was created during this call; otherwise `created_now=false`.
5. Prepare the execution environment (Section 9.7). It reports whether it created the issue's environment just now. Under `local` that is the directory's `created_now`; under `remote` nothing is prepared on the host.
6. If the environment was created now, run `after_create` hook if configured.

Notes:

- This section does not assume any specific repository/VCS workflow. The `remote` environment with a repository set is the one built-in exception: Sortie seeds each job from its repository mirror (Section 9.7.2).
- Workspace preparation beyond directory creation (for example dependency bootstrap, checkout/sync, code generation) is implementation-defined and is typically handled via hooks.

### 9.3 Optional Workspace Population (Implementation-Defined)

Sortie does not require any built-in VCS or repository bootstrap behavior.

Implementations may populate or synchronize the workspace using implementation-defined logic and/or hooks (for example `after_create` and/or `before_run`).

Failure handling:

- Workspace population/synchronization failures return an error for the current attempt.
- If failure happens while creating a brand-new workspace, implementations may remove the partially prepared directory.
- Reused workspaces should not be destructively reset on population failure unless that policy is explicitly chosen and documented.

### 9.4 Workspace Hooks

Supported hooks:

- `hooks.after_create`
- `hooks.before_run`
- `hooks.after_run`
- `hooks.before_remove`

Execution contract:

- A hook and the reaction triage command start only once the workspace path has been verified, immediately before the start, as a directory rather than a link (Invariant 9); a failed verification takes that hook's own failure semantics below.
- Execute through the workflow's execution environment (Section 9.7), with the workspace directory as `cwd`. Under `local` that is a local shell context appropriate to the host OS. Under `remote` the hooks are refused (Section 9.7.1). The platform rules below describe the `local` kind.
- On POSIX systems, `sh -c <script>` is the conforming default; `bash -lc <script>` may be used when a login shell environment is required. Termination when the shell exits, times out, or is cancelled kills the entire process group, not just the direct child, through a drain that runs before the shell is reaped and resends the kill until no process of the group other than the exited shell is alive, bounded at 2 seconds.
- On Windows, `cmd.exe /C <script>` is the conforming default. The hook subprocess is created suspended and assigned to a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` before it is resumed, so no unsupervised execution window exists in which a spawned descendant could start before the job assignment takes effect. Termination when the shell exits, times out, or is cancelled kills the entire process tree, not just the direct child, followed by a drain that resends the termination until the job reports no active process, bounded at 2 seconds. Job Object creation is not a precondition for running the hook: when it fails, the failure is logged, the hook still runs, and that run has no process-tree termination guarantee.
- When the shell exits, for any reason, every process still in its process group (POSIX) or Job Object (Windows) is terminated before the hook's outcome is reported, whenever that Job Object exists; the drain each platform runs above resends until no process of the group or Job Object is still alive, or, when a member is still alive once its bound elapses, is reported as a warning that leaves the tree unproven rather than torn down. A Windows hook running without a Job Object, because it could not be created or assigned, has only its direct process reached by that teardown. A process meant to outlive the hook is started by a supervisor outside that tree, such as a service manager or a container engine: on POSIX a process that starts its own session also leaves the tree, and on Windows a process leaves the Job Object only when something outside the job starts it.
- A hook's outcome is decided by the shell's exit status, the timeout, and cancellation. Output that a process outside the tree keeps open after the shell exits is captured only until the 5-second drain bound, and the cut is logged once.
- Hook timeout uses `hooks.timeout_ms`; default: `60000 ms`.
- Log hook start, failures, and timeouts. A failure or timeout record carries the hook's captured combined stdout and stderr under `hook_output`; a hook that succeeds with output emits it in a debug-level record.
- Output capture retains the last 8 KiB of the combined stream (the tail, where failure diagnostics appear), prefixed with a truncation marker when earlier output was dropped.

Failure semantics:

- `after_create` failure or timeout is fatal to workspace creation.
- `before_run` failure or timeout is fatal to the current run attempt.
- `after_run` failure or timeout is logged and ignored.
- `before_remove` failure or timeout is logged and ignored.

Hook environment variables available to all hooks:

- `SORTIE_ISSUE_ID`: tracker-internal issue ID.
- `SORTIE_ISSUE_IDENTIFIER`: human-readable ticket key.
- `SORTIE_WORKSPACE`: absolute path to the per-issue workspace directory.
- `SORTIE_ATTEMPT`: current attempt number.
- `SORTIE_ENVIRONMENT`: the environment kind, set only in an environment that runs elsewhere. No built-in kind runs hooks elsewhere: `remote` refuses them.

Hook environment variables available only to `after_run`:

- `SORTIE_SELF_REVIEW_STATUS`: self-review outcome for the current run: `"disabled"`, `"passed"`, `"cap_reached"`, or `"error"`. Defaults to `"disabled"` when self-review is not configured.
- `SORTIE_SELF_REVIEW_SUMMARY_PATH`: absolute path to `.sortie/review_summary.md` in the workspace. Absent when self-review did not run or summary was not written.
- `SORTIE_RUN_OUTCOME`: how the attempt ended: `"succeeded"`, `"stopped"` (the run needed input, stopped at a budget or a usage limit, or the agent declared `blocked`), `"failed"`, or `"cancelled"`. The orchestrator sets it in every mode, and it overrides a same-named variable of the parent environment. The value is decided before the handoff evidence verdict (Section 9.4.1), so an attempt whose handoff the policy then withholds reads `"succeeded"` in the hook and is recorded as failed. A cancellation that lands while the hook runs can make the hook read `"failed"` for an attempt recorded as cancelled. A hook that commits and pushes can skip every value other than `"succeeded"`.

#### 9.4.1 Handoff Evidence Baseline

For a run whose frozen `tracker.handoff_evidence` policy is `observed` or `strict`, the orchestrator captures a workspace baseline at the boundary between preparation and agent launch: after directory creation/reuse, population, and the applicable pre-run hooks have finished, and before `StartSession` can begin agent work. Both the full preparation path and the ensure-only path must reach this boundary. Under `off`, no baseline is captured.

The baseline records the workspace's committed position and enough working-tree state to determine later whether that state changed. Evidence is a difference from this baseline, not a property of the tree at exit:

- A moved committed position or a working-tree state that differs from the baseline is work observed.
- Workspace SCM metadata naming a pushed commit or pull request for the issue is also work observed, even when an earlier run wrote that metadata. Per-issue workspaces are reused, so this positive signal prevents a later no-op run from stranding work that was already pushed.
- When no positive signal holds, a successful inspection against a baseline is absence of work observed. A missing baseline, a non-version-controlled workspace, a failed inspection, or no recorded workspace yields evidence not determinable instead.

No particular source-control command or baseline representation is prescribed. The contract must distinguish pre-existing uncommitted state from changes made by this run and must recognize a commit created by `after_run` even when that hook leaves the tree clean.

The inspection runs through the execution environment, like the hooks: an environment that runs elsewhere runs every inspection `git` command in its workspace, and Sortie only reads its output bytes, with the same caps; no `git` process starts on the host for it. `remote` runs none. Untracked files enter the working-tree fingerprint as the object IDs that `git hash-object --stdin-paths` prints for them in the environment, not as file contents read on the host. A path it cannot hash, such as a dangling link, is recorded as unhashable.

The `after_run` contract itself is unchanged. Its failure remains logged and ignored, and its exit status has no vote in the handoff disposition. Any files, commits, pushes, or SCM metadata the hook produces are visible only through the same workspace evidence rules as other run output.

#### 9.4.2 Reaction triage command

The reaction triage command is a fifth operator-owned script that runs in a per-issue workspace, distinct from the four hooks above. A reaction kind that carries a `triage` block runs it once the reconcile pass has found a new subject and before it dispatches an agent continuation, so deterministic work is resolved without spending an agent session. Four reaction kinds offer the block; Section 5.3.8 names them and Section 2.9 of the workflow reference gives the field, input, and result contracts.

It executes through the same machinery as the hooks: the execution environment's shell with the workspace directory as `cwd`, the same restricted environment, the same process-tree termination when the command exits, times out, or is cancelled, and the same 8 KiB captured output tail. Three differences are load-bearing.

- It runs on its own goroutine rather than on the poll loop, and its timeout is `reactions.<kind>.triage.timeout_ms` rather than `hooks.timeout_ms`. A reconcile pass never blocks on it: the pass that starts a run re-enqueues the entry, and a later pass reads the answer.
- It never creates the workspace directory. A missing directory ends the run before a subprocess starts, and the reaction dispatches the agent as it would have without the block. Creating the directory would make the next dispatch see an already-existing workspace, skip `after_create`, and run `before_run` in an empty tree.
- Every failure is non-fatal. A missing workspace, a failed start, a timeout, a non-zero exit, a missing, oversized, or malformed result file, and an unrecognized disposition each produce one warning record naming the reason and each fall back to `dispatch-agent`, so a broken script costs one log record and one agent turn rather than a stranded reaction.

Environment variables, in addition to the four every hook receives and the `SORTIE_SSH_HOST` value hooks receive when a host preference is set:

- `SORTIE_REACTION_KIND`: the runtime reaction discriminator.
- `SORTIE_REACTION_INPUT`: absolute path to a readable JSON document describing the subject. Externally authored text, including review comment bodies, CI check names, and branch names, reaches the command only through this file, never through an environment variable or a shell word.
- `SORTIE_REACTION_RESULT`: absolute path the command writes its one-object JSON answer to. The file does not exist when the command starts.

Both paths sit in a directory created per run and removed when the run returns. Under `local` it is a fresh host temporary directory. Under an environment that runs elsewhere it is `/run/sortie/triage/<run id>/` inside the environment, mode `0700`, on the guest's tmpfs, or `triage/<run id>/` under the transfer directory an environment names through `TransferDirNamer`, `/tmp/sortie-transfer` in a VM whose machine user is set: Sortie resumes a stopped VM, writes the input by piping it to a command in the guest, reads the result back through a bounded command, and removes the directory, and whatever a run leaves under `/run/sortie` goes when the VM stops. The input's `workspace` member is the guest path. Either way the directory is outside the workspace, so no stale result file from an earlier run can be read as this run's answer and no externally authored text lands where the agent reads.

Sortie records every answer it applies, once per run, in a durable record keyed by the issue, the reaction kind, and the subject's fingerprint, so the dashboard and the API read a `handled` answer after a restart. A recorded answer is never replayed: the record serves only reads, and a subject whose dispatched mark never landed is triaged again from scratch. Runs in flight stay runtime-only.

Triage runs in flight at one time are capped at `max(agent.max_concurrent_agents, 1)` across every issue and kind. An entry that finds the cap reached starts nothing and is reconsidered on the next tick. The cap adds to agent concurrency rather than sharing it: triage runs are not subtracted from agent slots, so a host configured for N agents reaches a worst case of N agent processes and N triage subprocesses at the same time. An operator sizing a host accounts for both. Deriving the cap from the free agent slots instead would drive it to zero exactly when every slot is busy, which is when a triage run that resolves a subject without an agent is worth the most.

Shutdown drains in-flight triage runs. Cancellation reaches the subprocess through the same process-group termination the hooks use, so a script MUST tolerate being killed at any instruction: the same kill also fires on timeout and whenever a pass computes a different fingerprint for the subject.

### 9.5 Workspace SCM metadata (`.sortie/scm.json`)

The `.sortie/scm.json` file is a workspace-level file that carries SCM metadata written by the agent, a post-push hook, or any process running inside the workspace. The orchestrator reads this file after a normal worker exit to determine the git ref for CI status queries and the PR identity for review comment polling. The file is a shared workspace-level SCM metadata contract that CI feedback, review comment routing, and other features can reuse.

`SCMMetadata` fields:

- `branch` (string, required for CI): the branch name (e.g. `feature/PROJ-42`). If empty or absent, the file is treated as missing and CI status queries are skipped.
- `sha` (string, optional): the commit SHA at push time. When present, the orchestrator passes this to `CIStatusProvider.FetchCIStatus` instead of the branch name for deterministic results.
- `pushed_at` (string, optional): ISO-8601 timestamp of the push. Startup pending reaction recovery uses this timestamp to skip stale SCM activity. When absent, recovery falls back to `run_history.completed_at`.
- `pr_number` (integer, optional): the pull request number associated with this branch. Zero or absent when no PR has been created. Written by the agent or post-push hook. When positive and `owner` and `repo` are non-empty AND `reactions.review_comments.provider` is configured, the orchestrator creates a pending `review`-kind reaction on normal worker exit. When positive and `owner` and `repo` are non-empty AND `reactions.auto_merge.provider` is configured, the orchestrator creates a pending `merge`-kind reaction on normal worker exit. When `pr_number` is `0`, both review polling and auto-merge are skipped.
- `owner` (string, optional): the SCM repository owner (e.g. GitHub org or user). Written by the agent alongside `pr_number`. Required for review comment polling; when empty, review polling is skipped. The `owner` field is the authoritative source of SCM repository identity; it is never derived from the tracker project configuration, which may be a Jira key or other non-SCM identifier.
- `repo` (string, optional): the SCM repository name. Written by the agent alongside `pr_number`. Required for review comment polling; when empty, review polling is skipped.

Safety and parsing rules:

- Maximum file size: 4096 bytes. Oversized files are rejected and logged at warn level.
- Symlink rejection: the read goes through the workspace directory and `.sortie/` under Invariant 7. A link, a wrong entry type, or an entry replaced while it is being opened, at either directory or at the file, rejects the file with a warn record.
- Malformed JSON is rejected and logged at warn level.
- The function never returns an error to the caller; all failure modes degrade gracefully to a zero-value metadata struct (CI queries are skipped).

Under `remote` with a repository set, the agent does not push, so this file is not the source. Sortie writes the record itself after each push, to `<workspace>/.sortie-host/scm.json`, and every reader of SCM metadata reads that record and ignores `.sortie/scm.json` (Section 9.7).

#### 9.5.1 `.sortie/status` lifecycle

A run that enters the self-review phase has the file removed at four points in its lifetime, all best-effort and all subject to the same symlink rejection described for `.sortie/scm.json` above: no removal follows a link at the workspace directory, `.sortie/`, or the file; such a link stays in place and is logged, and a rejected or failed removal is logged and does not affect the run.

The first removal happens before each new dispatch to a workspace, so a stale value from a previous run cannot affect the new one. The second happens during a run, at the moment the orchestrator acts on a recognized value that admits the run to the self-review phase: the removal runs immediately before the phase's first review turn, so the phase's own first read does not observe the value that admitted it. The third and fourth happen inside the phase itself, after each review turn and after each fix turn, whenever the value read there is recognized. Together these four removals keep the file stating what the agent has said since the phase last acted on it, rather than carrying forward a value already acted on.

The read after each completed coding turn, outside the self-review phase, removes nothing: a recognized value read there is left in the file, and a run that never enters the phase carries that value through to teardown.

#### 9.5.2 Dispatch identity record (`.sortie/dispatch.json`)

The `.sortie/dispatch.json` file records, for the current dispatch, the latest session id the worker has accepted from the agent runtime. The worker keeps it current through the same writer every `.sortie/` file uses, and its write follows Invariant 7 below. The worker writes the record when the agent session starts, before each coding and self-review turn, whenever a relayed event reports a new session id, and whenever a coding or self-review turn's result reports a session id other than the accepted one, so a tool server process reading it always sees the worker's most recent acceptance for the dispatch that wrote it. Section 10.2 states the acceptance rule.

The record holds one JSON object with two fields, `dispatch_id` and `session_id`. A reader accepts the recorded session id only when the record's `dispatch_id` equals its own dispatch id, so a tool server process outliving its dispatch, or reading a record a later dispatch has since overwritten, never observes a session id that is not its own. The reader rejects a `.sortie` directory or a record that is a symbolic link or not the expected type, and also rejects a workspace directory that is a link or changes while being opened; it opens the record without waiting for a writer and reads at most 4096 bytes; any rejection yields an empty session id rather than an error. Like `.sortie/mcp.json`, the agent may also write this file directly.

### 9.5.3 Notification slot directory (`.sortie/notification_slots/`)

The `.sortie/notification_slots/` directory holds the shared count `notify_operator`'s rate limit enforces for one dispatch (Section 10.4.5). It is created, mode `0o750`, the first time any tool server process of the dispatch claims a slot; a dispatch that never calls `notify_operator` never creates it.

Layout: slot `n`, where `n` runs from `1` without leading zeros, has three names. The slot file `<dispatch_id>-<n>` is an empty regular file whose existence counts `n` against the cap; no other state records the count, and an entry of any type at that name counts. The lock file `<dispatch_id>-<n>.lock` is an empty regular file on which a call holds an exclusive operating-system file lock while it claims `n`. The pending file `<dispatch_id>-<n>.pending` is an empty regular file the claiming call creates while it holds that lock; it never counts.

A call claims the lowest `n` in `1..cap` whose slot file is absent and whose lock it can take without waiting. Once it holds the lock, it checks the slot file again and creates the pending file, first removing one that a claim whose process ended left behind; only the call that holds the lock touches the pending file. Every claim therefore makes an exclusive create before its notification is sent, so a slot directory that refuses creates stops the call with `state_unavailable` before delivery. A slot is occupied while its slot file exists, while another call holds its lock, while its lock-file name holds anything but a plain file with a single link, or while its pending-file name holds anything but a plain file. When a backend accepts the notification, the call renames the pending file to the slot file, or creates the slot file when the rename fails, and only then releases the lock, so `n` counts from that moment. When no backend accepted it, the call removes the pending file and releases the lock, which frees `n` for a later call.

A tool server process that ends while it holds a claim, however it ends, has its lock released by the operating system, and since it never created the slot file, `n` is free again. No clock, age, or process identifier frees a slot, so a claim whose process is alive is never taken over, however long its delivery takes. Windows releases a terminated process's lock after a delay it does not bound; until then `n` stays occupied, and a later call claims another `n` or returns `rate_limited` when none is free. A process that ends after a backend accepted its notification but before it created the slot file leaves `n` free, so that notification does not count against the cap. Neither does one whose slot file the directory refuses after delivery, and while that refusal lasts the next call to claim the same `n` stops before delivery.

When the lock cannot be taken for a reason other than another holder, the call logs a warning, claims `n` by creating the slot file exclusively before delivery, and removes that file when no backend accepted the notification. On such a filesystem a claim whose process ends before it resolves keeps its slot file, and the slot counts until the dispatch ends.

Every tool server process of the dispatch claims slots in this directory identically, whatever agent kind spawned it and whether it runs for one turn or the whole session; the shared count is what lets the cap bound one run instead of one process. The directory's lifetime matches the workspace's. No lock file is ever removed; a pending file is removed only by the call that holds its slot's lock, and a slot file only by its own call on a filesystem that refuses the lock, when the notification reached no backend. The directory keeps at most three files for each slot number a dispatch reached, until the workspace itself is removed.

The agent can write anywhere under `.sortie/`, so it can reset its own count by removing slot files, or exhaust it early by planting occupying entries or holding locks on lock files; the cap bounds a cooperating agent, not an adversarial one.

### 9.6 Safety Invariants

This is the most important portability constraint.

Invariant 1: Run the coding agent only in the per-issue workspace path.

- Before launching the coding-agent subprocess, validate:
  - `cwd == workspace_path`

Invariant 2: Workspace path must stay inside workspace root.

- Normalize both paths to absolute.
- Require `workspace_path` to have `workspace_root` as a prefix directory.
- Reject any path outside the workspace root.

Invariant 3: Workspace key is sanitized.

- Only `[A-Za-z0-9._-]` allowed in workspace directory names.
- Replace all other characters with `_`.

Invariant 4: A workspace key held by an entry in the running map or the retry map is never a sweep candidate, whatever its age, and so is removed by neither the terminal gate nor the age bound on that pass. These exclusions are absolute; the age bound does not weaken them.

Invariant 4a: A workspace key held by a pending reaction entry is a sweep candidate unless that entry's kind carries an expiry. A kind whose pending entry is bounded by a TTL pins its workspace for as long as the entry lives; a kind that polls an external fact for an unbounded time does not, because pinning it would hold the directory for as long as that poll runs. A reaction kind the orchestrator does not recognize pins, so an unclassified kind fails toward retention rather than removal. A kind that does not pin MUST NOT depend on the workspace directory surviving between its own polls. A kind whose pending entry is configured with an unbounded watch window (`watch_window_ms: 0`) still pins its workspace for as long as the entry lives; the entry's own terminal-state release remains the exit that ends the pin. Pinning stays keyed on kind, not on the configured value. An entry of a kind that pins keeps its workspace pinned while its escalation stands, bounded by the escalation's ending facts in place of the watch window, which never drops a standing escalation.

Invariant 5: `workspace.retention_days` cannot be configured below its floor, and that floor in days equals the pending-reaction recovery lookback in days. Any workspace the age bound may remove is one that pending reaction recovery would already have skipped as stale, so removing it cannot silently break recovery for an issue recovery still regards as live.

Invariant 6: The age bound performs no tracker write, no source-control write, no reaction fingerprint write, and no creation or deletion of a pending reaction entry. Reaction state is read-only to the age bound. Every removal it performs routes through the same workspace removal path as the terminal gate, so key sanitization, containment under the workspace root, and the `before_remove` hook apply unchanged. The age bound introduces no new way to reach the filesystem.

Invariant 7: Every write, read, and removal the orchestrator and the tool servers perform against a workspace's `.sortie/` directory runs through a verified handle on the workspace directory and on `.sortie/`. A write replaces its destination through an exclusive create under a fresh name and a rename, with every step resolved inside that verified `.sortie/`. A link, a non-directory, or an entry replaced while it is being opened, at either the workspace directory or `.sortie/`, fails the operation. The boundary the invariant holds is the verified `.sortie/`, not the workspace at large: a directory renamed into the workspace path is the workspace for every later operation, and putting a directory there takes rename access to the workspace path itself.

Invariant 8: Every slot file, lock file, and pending file a tool server process creates, opens, renames, or removes resolves through the same verified-handle boundary Invariant 7 holds, extended to a verified `.sortie/notification_slots/` handle. A link, a non-directory, or an entry replaced while it is being opened, at the workspace directory, `.sortie/`, or `.sortie/notification_slots/`, is refused. A new lock file or pending file is created only exclusively, which never follows a link, and the slot file is either created exclusively or produced by renaming the pending file's own entry, which replaces whatever the slot name holds. An existing lock file is opened only as the plain file verified at its name immediately before the open, without blocking on a pipe planted there; a link, a non-plain entry, a file whose link count is not one, or an entry replaced while it is being opened at that name counts its slot as occupied, and such a file is closed without being locked or written. Only the call that holds a slot's lock renames or removes its pending file; the removal refuses anything but a plain file, and the rename moves the entry itself, never a link's target. No lock file is ever removed, and the only other removal a tool server performs is of the plain slot file the same call created, on a filesystem that refuses the lock, when the notification reached no backend.

Invariant 9: Every subprocess started with a workspace as its working directory, whether the coding agent per session, per turn, or for an auxiliary command, a hook, the reaction triage command, a verification command, or a source-control inspection, starts only after the workspace path is verified, immediately before the start, as a directory rather than a link. The check is point-in-time, because a working directory is set by path rather than by a held handle.

Invariant 10: Under an execution environment that runs elsewhere, no workspace code and no `git` command against the guest repository runs on the host. Sortie runs host `git` only on its own bare mirror (Section 9.7), never on a repository the guest can write.

Invariant 11: Removal follows a recorded environment kind only when that kind is not `local`. A missing record, or a record that names `local`, is removed by the configured environment, so an agent that rewrites the shared record cannot move removal, and its `before_remove` hook, onto the host.

### 9.7 Execution Environments

An execution environment is where the code of an issue's workspace runs. The orchestrator resolves the configured `environment.kind` through the environment registry (`internal/environment`); the kinds are `local` and `remote`, each in its own package that registers itself. Every operation that runs workspace code builds its process through the environment: the agent session and the credential verification session (through the launch carrier, Section 10.10), auxiliary agent commands, the four hooks, self-review verification commands and the self-review diff, reaction triage, and handoff evidence inspection. There is no fallback: when the environment cannot be prepared, the preparation fails, and nothing of that issue runs on the host instead.

The environment interface:

- `Prepare(ws)`: make the issue's environment ready and report whether it was created just now, which gates `after_create`.
- `Command(ws, spec)`: return a host process that runs an argv in the workspace, in the environment, with the given variables.
- `Release(ws)`: called as the worker's last step on every exit.
- `Remove(ws)`: delete the environment; called wherever the workspace itself is removed, after `before_remove` and before the host directory goes.
- `Sweep(keep)`: delete leftover environments that no workspace references.

`local` builds a plain host process with the host workspace as `cwd`; every other method does nothing, so behavior is the same as without the block.

`smolvm`, which ran each issue workspace in a microVM on Sortie's own host, was removed. Validation refuses `environment.kind: smolvm` with an error on `environment.kind` that names the `remote` kind.

#### 9.7.1 The `remote` kind

`remote` runs nothing of an issue on Sortie's host: `Prepare` and `Command` refuse, and `Release`, `Remove`, and `Sweep` do nothing, because the worker owns the machine. Each dispatched attempt becomes a job in the worker matcher, a worker that dials Sortie runs the agent in the job's machine, and Sortie drives the session over the `stdio` data-plane flow. The worker API, the matcher, and the data plane are specified in `docs/fork.md` and the workflow reference. The four hooks are refused under `remote`, and the host directory `<workspace.root>/<key>` keeps the workspace's identity for every path rule of this section (containment, key sanitization, retention, the terminal gate) and holds only control files.

**The environment record.** `Prepare` writes `.sortie/environment`, `{"kind": "<kind>", "name": "<instance>"}`, for every kind but `local`; `name` is set for a kind that names its instance. `local` writes none, because a local workspace has no `.sortie` before `after_create`; a workspace without a record counts as created by `local`. When an issue is dispatched and its existing workspace's recorded kind differs from the configured one, or the record is unusable, the workspace is stale: the recorded kind's environment removes its instance (the configured kind's for an unusable record), the host directory is deleted, and preparation starts from scratch, so `after_create` runs again. A recorded kind that is no longer registered, such as `smolvm`, cannot be built, so preparation and removal of its workspace fail and keep it. The record may sit in a directory the agent can write: removal follows it only under Invariant 11.

#### 9.7.2 Repository sync

With `environment.remote.repository` or `repositories` set, Sortie moves git data across the boundary as bundles, and a worker needs no git credential and no route to the git host.

- **Mirror.** One bare mirror per URL at `<state dir>/repos/<sha256(url)[:16]>.git`, written only by Sortie, serialized by a lock file next to it. Every host `git` command on it disables hooks (`core.hooksPath=/dev/null`) and the filesystem monitor and checks every received object (`transfer.fsckObjects`, `fetch.fsckObjects`, `receive.fsckObjects`). It holds the upstream's `refs/heads/*`, `refs/sortie/incoming/<key>` (written only by an import), and `refs/sortie/pushed/<key>` (Sortie's last push). Host git is safe here because the mirror is never a work tree and objects enter it only through a checked fetch from a bundle. The upstream is reached with the host's own git configuration and credentials.
- **Issue branch.** `branch_prefix` followed by the workspace key, checked with `git check-ref-format --branch`; an invalid name fails preparation with the name.
- **Seed** (before the job is queued): fetch the upstream into the mirror, failing preparation on error; bundle `HEAD` and `refs/heads/<issue branch>`, the upstream issue branch when it exists and otherwise a new branch at `base`. The worker fetches the seed on the `bundles` flow with the attempt's job token.
- **Return** (after the session stops): the worker posts a bundle of `refs/heads/<issue branch>` that leaves out the seed's commits, up to `max_bundle_mib`, with the agent's `.sortie/pr.json` beside it. Sortie requires that `git bundle list-heads` names exactly that ref and that the branch is the seed's commit or on top of it, and imports it into `refs/sortie/incoming/<key>`. A missing branch, or a branch equal to the upstream branch, or equal to the base when no upstream branch exists, exports without a push. Only committed work is exported.
- **Push** with `--force-with-lease` against `refs/sortie/pushed/<key>`, or against absence when Sortie never pushed. A rebase of Sortie's own push is allowed; a branch someone else moved fails the export with `branch moved upstream`, leaving the upstream unchanged. A successful push updates `refs/sortie/pushed/<key>`.
- **Pull request.** With `repository.provider` set, Sortie calls the SCM adapter's optional `domain.PullRequestOpener.EnsurePullRequest` for the issue branch against `base` in `project`, with the returned `.sortie/pr.json` text or the default text (Section 21.3).
- **Failure.** A failure in any export step fails the run with an export error: no tracker transition, the normal retry backoff.
- **Trusted SCM record.** Sortie writes `branch`, `sha`, `pushed_at`, and, once known, `pr_number`, `owner`, and `repo` to `<workspace.root>/<key>/.sortie-host/scm.json` through `workspacekit` (`OpenHostDir`, `ReadHostFile`, `WriteHostFile`), with the same link refusal. With a repository set, every SCM metadata reader (CI, review and bot-review comments, auto-merge, merge conflicts, label commands, merge completion, the age bound, handoff evidence, and startup recovery) reads this record and ignores `.sortie/scm.json`. A record write that fails after a successful push is repaired on the next run, which finds the branch already upstream.
