## 11C. Auto-merge Reaction Contract

The auto-merge reaction is default-off; enablement requires `reactions.auto_merge.provider` to be non-empty in `WORKFLOW.md`. The merge operation is irreversible: the orchestrator does NOT roll back on partial-failure tail operations (branch delete, success-event delivery). The reaction runs after the CI, review-comment, bot-review-comment, and merge-conflict reconcile passes in the tick, so the precondition reads observe the most current per-kind state; it runs before the label-review and label-fix passes, whose relative ordering does not affect correctness because each of those two is fully cross-kind isolated.

### 11C.1 SCMAdapter write surface

The `SCMAdapter` interface, introduced in §11B.1 for review-comment fetching, is widened by the read-write surface defined by the interface-surface section of the auto-merge ADR. The methods this reaction depends on are:

```text
SCMAdapter (auto-merge surface):
  FetchPendingReviews(ctx, prNumber, owner, repo) ([]ReviewComment, error)
  GetReviewDecision(ctx, prNumber, owner, repo)   (ReviewDecision, error)
  GetCIStatus(ctx, prNumber, owner, repo)         (string, error)
  GetMergeability(ctx, prNumber, owner, repo)     (PRMergeStatus, error)
  MergePR(ctx, prNumber, owner, repo, strategy, commitTitle, commitMessage, expectedHeadSHA) (MergeResult, error)
  DeleteBranch(ctx, owner, repo, branch)          error
```

`FetchPendingReviews` is the original read-only method documented in §11B.1. The remaining methods are the write surface the auto-merge reaction added. The interface has since widened further for other reaction kinds; see §11D for `FetchBotReviewComments` and §11F for `ListLabelEvents` and `RemoveLabel`. All implementations MUST be safe for concurrent use.

`ErrSCMConflict` is produced by a merge write path only: `MergePR` promotes a rejection to this kind, and no read method, `RemoveLabel`, or `DeleteBranch` returns it. The already-merged marker (§11C.2) is produced by the implementation itself, after it has confirmed by re-reading the pull request that the pull request is merged, rather than by matching the provider's response wording, so the disposition survives a reworded rejection and never fires on an unrelated conflict that happens to carry the word "conflict".

### 11C.2 Domain types

The auto-merge reaction introduces the following domain types:

- `MergeStrategy`: merge strategy for `MergePR`. One of `merge`, `squash`, `rebase`.
- `ReviewDecision`: normalized review status. One of `CHANGES_REQUESTED`, `REVIEW_REQUIRED`, `APPROVED`, `REVIEW_REQUESTED`, `NOT_REQUIRED`. An adapter that folds the forge's signals into one value returns the first that holds in this order, through one shared fold:
  - `CHANGES_REQUESTED`: a changes request stands.
  - `REVIEW_REQUIRED`: no changes request stands and an approval rule is unmet, counted as the forge's merge gate counts approvals. A requested reviewer alone never yields it.
  - `APPROVED`: neither of the above, and the forge reports approval or its approval rules met. GitLab Enterprise reports approval when no rule applies, so the value does not prove a person approved.
  - `REVIEW_REQUESTED`: none of the above, and a requested or assigned reviewer has not approved. Only adapters that fold reviewer requests return it (GitLab, Gitea).
  - `NOT_REQUIRED`: none of the above.
- `MergeabilityState`: PR mergeability classification. One of `clean`, `unstable`, `blocked`, `dirty`, `unknown` (lowercase string values).
- `PRMergeStatus`: struct carrying the merge precondition state. Fields: `ReviewDecision`, `CIConclusion`, `Draft` (bool, separate from `Mergeability`), `Mergeability` (`MergeabilityState`), `HeadSHA`, `BranchName`, `BaseBranch` (the PR's target branch, added for merge-conflict detection, §11E.1), `Merged` (bool reporting whether the pull request has merged, added for the post-merge closure reaction, ADR-0017), `MergeCommitSHA` (the merge commit identifier, set only when `Merged` is true, added by the same reaction), and `Closed` (bool reporting whether the provider considers the pull request no longer open, primarily read by the CI reaction, §11A.9), and `URL` (the pull request's web page as the provider reports it, empty when it reports none; each adapter normalizes its own wire field at its boundary, and the reactions keep the link to show a pending reaction's pull request). `ReviewDecision` and `CIConclusion` are unset by `GetMergeability`; callers obtain those from dedicated reads.
- `MergeResult`: struct carrying `SHA` (merge commit SHA), `Merged` (bool reporting whether the merge completed), and `Message` (provider-supplied status text). The already-merged case is NOT signaled on this struct; it is signaled via `*SCMError` with kind `ErrSCMConflict` and the substring `already merged` (case-insensitive) in the error message. Callers disambiguate that subcase as documented in §11C.3.

**GitLab caveat.** On GitLab the effective merge strategy is the project's own `merge_method` setting rather than a per-call choice. The adapter always calls the one merge route and never the asynchronous standalone rebase route, so a configured `rebase` performs the operator's intent on a project set to fast-forward or semi-linear history and produces a merge commit on a project set to merge commits. The adapter logs one WARN per merge naming that governance.

### 11C.3 Error kind

`ErrSCMConflict` is an `SCMErrorKind` value alongside the transport, auth, API, not-found, and payload categories already defined for review-comment fetching (§11B.3). Its HTTP semantics (405 method-not-allowed and 409 conflict from the merge endpoint) are documented in the §11B.3 table.

The auto-merge reconcile loop imposes merge-specific dispositions on this kind. A rejection the adapter has confirmed by re-reading the pull request to be an already-completed merge carries the already-merged marker and is treated as idempotent success. Any other 405 or 409, whether head-SHA drift or a branch-protection refusal, is re-enqueued with the poll interval; the next tick observes the new SHA via a refreshed fingerprint. The full disposition table is in §11C.5 and §11C.6.

### 11C.4 Reconcile loop integration

The auto-merge reconcile loop runs as Part G of active run reconciliation (see §8.5 Part G for the algorithm). The loop processes entries from `pending_reactions` whose kind discriminator is `"merge"`.

The loop drops an entry whose age, measured from the entry's creation, exceeds the configured `reactions.auto_merge.watch_window_ms` (default `1800000`, thirty minutes; `0` removes the bound), and deletes the `merge`-kind attempt counter with it. The drop logs at WARN and performs no escalation, so an entry that never satisfies its preconditions produces no tracker-visible signal. The window never drops an entry whose escalation stands (§11C.6), nor, under plan control, the entry of a `sortie-review:ui` story, which ends when its pull request merges or closes.

The loop also drops an entry whose `GetMergeability` read reports the pull request already merged, whatever its mergeability: a pull request a person or a forge merged first reads `unknown` on some forges, and without this drop the merge call would never run and the entry would live until its watch window ends, or without end under `0`. The drop records the merged outcome with the `merge` path (Section 4.1.8), deletes the `merge`-kind attempt counter and fingerprint, and logs at INFO. It performs no escalation, emits no `auto_merge.merged` event, and deletes no branch.

An entry whose escalation stands is spared by both the window and the token-scope preflight drop (§11C.9). Each due pass still reads the pull request for it, also after the preflight failed: a merged read ends the escalation and drops the entry as the drop above does, and a closed or missing pull request ends the escalation and drops the entry and its attempt counter. No other read changes the entry, and the pass makes no merge call.

Under plan control, a run that ends parked seeds no entry. While a story is parked with an open pull request, the poll tick reads that pull request's conversation comments at most once per auto-merge poll interval; when they hold, at its current head, every record its latest criteria verdict leaves open (click-proofs for a `sortie-review:ui` story, the live proof for a `live-proof` verdict, both for a ui story with a `live-proof` verdict), the park is released with the gesture `record` and no session, the story moves to `tracker.handoff_state`, and the CI, merge-conflict, auto-merge and merge-completion entries are seeded at that head as a passed criteria check seeds them. A record naming another commit, the wrong kind of record, a refused verdict or no verdict leaves the park held. The seeded entry then follows this section's rules like any other.

There is an intentional asymmetry between the YAML configuration key and the runtime kind value. The YAML key the operator sets in `WORKFLOW.md` is `reactions.auto_merge`. The runtime and persisted kind value used in `pending_reactions` map keys and in the `reaction_fingerprints` table's `kind` column is `"merge"`, not `"auto_merge"`. A reader who only sees the YAML key would expect the runtime kind to be `auto_merge`; it is `merge`.

### 11C.5 Merge precondition state machine

The auto-merge reconcile loop evaluates the merge preconditions reported by `GetMergeability` (via `PRMergeStatus`) alongside the review-decision and CI-conclusion reads before calling `MergePR`. The transition table below summarizes the loop's decisions. `Draft` is the `PRMergeStatus.Draft` boolean (not a `MergeabilityState` value); `Mergeability` is the `PRMergeStatus.Mergeability` field:

| From state | Observation | To state | Action |
|------------|-------------|----------|--------|
| Pending | `Merged == true` | Done | Record the merged outcome (Section 4.1.8); remove the pending entry; clear the attempt counter and the fingerprint. Emit no event, delete no branch, and increment no metric. |
| Pending | `Draft == true` | Pending | Re-enqueue with poll interval. |
| Pending | `Mergeability == dirty` | Pending | Re-enqueue with poll interval. |
| Pending | `Mergeability == blocked` | Pending | Re-enqueue with poll interval. |
| Pending | `Mergeability == unknown` | Pending | Re-enqueue with poll interval (transient state). |
| Pending | `Mergeability` not in (`clean`, `unstable`) | Pending | Re-enqueue with poll interval. |
| Pending | `ReviewDecision != APPROVED` and `!= NOT_REQUIRED` | Pending | Re-enqueue with poll interval. |
| Pending | `require_ci == true` and `CI` is neither `success` nor the empty no-checks value | Pending | Re-enqueue with poll interval. |
| Pending | Under plan control, a `sortie-review:ui` story whose pull request is closed unmerged | Done | Remove the pending entry; clear the attempt counter. |
| Pending | Under plan control, a `sortie-review:ui` story whose other preconditions hold and whose pull request holds no click-proofs record naming its head SHA | Pending | Re-enqueue with poll interval; comment on the story, once per head SHA, asking for the click-proofs on the preview. A failed conversation read re-enqueues with backoff. |
| Pending | Under plan control, a story whose latest criteria verdict is `live-proof`, whose other preconditions hold and whose pull request holds no `sortie:live-proof proven` record naming its head SHA | Pending | Re-enqueue with poll interval; the review gate lets the `live-proof` verdict through only with that record. A failed conversation read re-enqueues with backoff. |
| Pending | All preconditions satisfied (`Mergeability` in (`clean`, `unstable`); `!Draft`; review and CI satisfied; for a ui story, its click-proofs recorded; for a `live-proof` verdict, its live-proof recorded) | Merging | Call `SCMAdapter.MergePR`. |
| Merging | Merge succeeded | Done | Record the merged outcome (Section 4.1.8); emit the `auto_merge.merged` event, whose body is the merge-success comment, to its subscribers (§11C.6); delete branch when `delete_branch == true`; clear fingerprint; increment `sortie_reactions_auto_merge_total{result="merged"}`. |
| Merging | `ErrSCMConflict` ("already merged") | Done | Treat as idempotent success; same actions as merge succeeded. |
| Merging | `ErrSCMConflict` (head SHA mismatch) | Pending | Re-enqueue with poll interval; next tick refreshes fingerprint. |
| Merging | `ErrSCMAuth` | Escalated | Escalate immediately with trigger `refused`; the entry stays pending. |
| Merging | `ErrSCMPayload` | Escalated | Escalate immediately with trigger `refused`; the entry stays pending. |
| Merging | Other transient error | Pending | Re-enqueue with backoff; escalate with trigger `budget_exhausted` on a later tick when `MaxRetries > 0` and the attempt count reaches it (§11C.6). |
| Escalated | `Merged == true` | Done | End the escalation; record the merged outcome (Section 4.1.8); remove the pending entry; clear the attempt counter. Emit no event, delete no branch, and increment no metric. |
| Escalated | Pull request closed or missing | Done | End the escalation; remove the pending entry; clear the attempt counter. |
| Escalated | Issue leaves `tracker.handoff_state` while Sortie does not drive it | Done | End the escalation; remove the pending entry; clear the attempt counter. |
| Escalated | Any other observation | Escalated | Re-enqueue with poll interval. Make no merge call and apply no action. For a count-based escalation, a ceiling that no longer counts the stored tries as spent ends the escalation and the pass goes on as for a pending entry. |

**Gitea auto-merge reads.** The Gitea adapter has no aggregate review-decision field and no `mergeable_state` string, so it composes both preconditions itself. `GetReviewDecision` folds the per-review list, taking each reviewer's latest approving or changes-requested review, normalizing Gitea's native `REQUEST_CHANGES` review state to this contract's `CHANGES_REQUESTED`, and combining the result with the base branch's `required_approvals` and the PR's requested-reviewers signal into one decision. It reads the base branch's protection through the repository branch route: `REVIEW_REQUIRED` means `required_approvals` exceeds the number of reviewers whose latest approving or changes-requested review is an approval Gitea marks official, a stale approval counts, and a requested reviewer on an unprotected base yields `REVIEW_REQUESTED`. A 404 from the branch route folds as no rule, any other failure of it fails the read, and a `required_approvals` that is not an integer fails the read with `scm_payload_error`. Auto-merge on Gitea therefore waits for the approvals the rule requires instead of attempting a merge the forge refuses. `GetMergeability` maps Gitea's single `mergeable` boolean to `clean` (mergeable and not a draft), `blocked` (draft), or `unknown` (anything else), and never yields `dirty` or `unstable`. Gitea cannot distinguish a merge conflict from an in-progress recheck, so both present as `unknown`, which this table re-enqueues on the poll interval as a transient state. The fold orders decision-bearing reviews by submission time, so that timestamp is load-bearing; a review that can change the verdict and carries a submission timestamp that is not a valid RFC 3339 value fails the read with a `scm_payload_error` rather than sorting to the epoch, because a substituted timestamp can let a superseded approval outrank the changes-requested review that supersedes it. A review that cannot change the verdict, a dismissed review or one whose state is not a decision, is not parsed and cannot fail the read. The precondition read defers with backoff and never merges on this failure, and the pending entry is dropped once its age passes the configured watch window (§11C.4), which escalates nothing and leaves the pull request unmerged for the operator to merge manually.

**GitLab auto-merge reads.** The GitLab adapter has no aggregate review-decision field and no per-condition mergeability enum, so it composes the first and maps the second from a single string. `GetReviewDecision` folds two reads: a reviewer whose review state is `requested_changes` returns `CHANGES_REQUESTED` before the approvals payload is read at all, then the approvals payload decides: `approvals_left` above zero returns `REVIEW_REQUIRED`, an approved payload returns `APPROVED`, a non-empty reviewer list returns `REVIEW_REQUESTED`, and none of these returns `NOT_REQUIRED`. A negative `approvals_left` is not unmet, and a non-integer one fails the read with `scm_payload_error`. The changes-requested arm returns unconditionally, so a later approval by a second reviewer never clears an outstanding block. Community Edition carries no `approvals_required` and no `approvals_left` on the merge request, exposes no approval-state route, and has no approval rules at all, so it can never require an approval: a merge request with no reviewer assigned folds to `NOT_REQUIRED`, and this table proceeds to the merge. A reviewer assigned on Community Edition folds to `REVIEW_REQUESTED`, which this table defers on, so enforcing review there means assigning a reviewer, because the platform offers no server-side alternative. `GetMergeability` maps GitLab's single `detailed_merge_status` value: `mergeable` to `clean`, `conflict` to `dirty`, `unchecked`, `checking`, `preparing`, and `approvals_syncing` to `unknown`, and every other value to `blocked`, logging at WARN only a value outside the set the platform documents. It never yields `unstable`, because a pipeline whose only failing job is allowed to fail reports success and leaves the merge request `mergeable`, and this table treats `clean` and `unstable` identically. The value names one blocking condition at a time and recomputes: approving a merge request has been observed to move it from `mergeable` to `checking` and back, so a read landing mid-recompute reports `unknown` and re-enqueues as a transient state. A `dirty` read is a mergeability observation rather than an error, re-enqueued on the poll interval, so the conflict error kind stays confined to the merge write path.

### 11C.6 Escalation behavior

The count-based escalation check guards the comparison with `MaxRetries > 0`: the orchestrator escalates the issue when `MaxRetries > 0` and `reaction_attempts[issue_id:merge] >= MaxRetries`, or immediately when `ErrSCMAuth` or `ErrSCMPayload` is returned on `MergePR` (both paths invoke the escalation directly, bypassing the `MaxRetries` check). `MaxRetries` defaults to `2`.

A configured `MaxRetries` of `0` disables count-based escalation rather than triggering it on the first attempt: the `> 0` guard keeps the comparison false no matter how large `reaction_attempts[issue_id:merge]` grows, so a `merge`-kind entry with no retry budget keeps retrying transient failures instead of escalating on them. This is the opposite of the sibling merge-conflict reaction, whose comparison (`attempts > MaxRetries`, §11E.5) carries no such guard, so a merge-conflict `MaxRetries` of `0` escalates on the first conflict detection (§11E.8). The same `0` literal therefore carries two incompatible meanings across these two adjacent `WORKFLOW.md` configuration blocks. An `ErrSCMAuth` or `ErrSCMPayload` failure still escalates immediately regardless of the configured `MaxRetries`, including when it is `0`.

Every escalation emits the `escalation.auto_merge` event (Section 10.4.7) under every posture, in a detached goroutine with a 30-second timeout. The event body is a plain-text message identifying the PR number, the retry count, and that manual merge is required. Three escalation postures are available, set by the operator in `WORKFLOW.md`:

- `label`: applies the `needs-human` label to the tracker issue via `TrackerAdapter.AddLabel`. This is the default posture, and it is the posture an omitted `escalation` resolves to. The event reaches only destinations that subscribe to `escalation.auto_merge`, so the issue receives no comment unless a `tracker_comment` entry lists it.
- `comment` (deprecated): applies no label; the `tracker_comment` destination receives the event through a subscription Sortie synthesizes from this value. The comment keeps posting and a deprecation advisory names the replacement: `escalation: none` plus `escalation.auto_merge` in the events of a `tracker_comment` entry (Section 5.3.10).
- `none`: applies no label; the event reaches only destinations subscribed to it.

The merge-success comment is the `auto_merge.merged` event (§11C.5): a `tracker_comment` entry that lists it posts the comment, and a configuration with no `tracker_comment` entry keeps posting it through an implicit subscription that draws a deprecation advisory. A `tracker_comment` entry that omits `auto_merge.merged` posts no merge-success comment.

Neither posture writes to the reaction attempt state. The escalation records on the `merge`-kind pending entry its time, its trigger, and its subject, and stores that record. The trigger is `budget_exhausted` for the count check and `refused` for `ErrSCMAuth` or `ErrSCMPayload`. The subject is the merge fingerprint (§11C.7) of the call that escalated, and empty for the count check. The configured action applies when the stored subject differs from this one or this one is empty, so one fingerprint gets one action however often its escalation ends and begins again. The `reaction_fingerprints` row is removed, the entry stays pending, and the `reaction_attempts[issue_id:merge]` counter stays in place.

What makes escalation terminal is a fact that ends it, and the escalation stands until one holds. The orchestrator stores the escalation, so it survives a restart with its time, trigger, subject, and counter (Section 14.3). While it stands, the entry makes no merge call, applies no action, and is never dropped by the watch window (§11C.4) or the token-scope preflight drop (§11C.9). The escalation ends on any of these facts, and on no other:

- The pull request merges, which the pass records as the merged outcome the drop in §11C.4 records, or the pull request closes, or the pull request is missing (`ErrSCMNotFound`).
- The tracker issue reaches a terminal state.
- A person moves the issue out of `tracker.handoff_state`, or releases its park so Sortie runs the issue again, while Sortie does not drive it (Section 8.5 Part B).
- For a `budget_exhausted` escalation, the ceiling as the pass applies it no longer counts the stored tries as spent. This kind counts a counter as spent only under a ceiling above `0`, so a ceiling of `0`, or a ceiling above the count, ends the escalation.

The counter leaves with the entry on every one of these exits. Leaving the handoff state deletes it, so an entry re-created after a person's answer no longer starts at the ceiling and escalates on its first tick.

Cross-kind isolation: escalation cleanup MUST scope deletions to the `merge` kind only. The escalation path MUST NOT mutate `ci` or `review` reaction state. The full normative invariant is in §11C.10.

### 11C.7 Fingerprint

Before calling `MergePR`, the reconcile loop computes a merge fingerprint as SHA-256 over the head SHA, a newline byte, and the review-decision string (for example: `sha256(headSHA + "\n" + reviewDecision)`). The fingerprint is upserted into the `reaction_fingerprints` table with `kind = "merge"`.

The fingerprint's purpose is to let a changed pull request re-arm the merge, not to suppress a retry of an unchanged one. The upsert carries that behavior: a computed value differing from the stored one resets the row's `dispatched` flag, and an unchanged value preserves whatever flag the row already holds, so a new push or a changed review decision produces a new fingerprint and a fresh attempt. The flag is set only after a merge the loop treats as complete, which is a successful `MergePR` return or the already-merged rejection of §11C.3. A failed attempt deliberately leaves the flag clear, so a transient or transport-class failure retries on the next due tick instead of being skipped as a duplicate.

These exits delete the row: a successful merge, the already-merged idempotent success, the drop of an entry whose pull request reads merged, and escalation. A merge, an already-merged success, and a merged read end this kind's work on the issue and remove the `merge`-kind pending entry in the same tick. An escalation deletes the row and no longer removes the entry, which makes no merge call while the escalation stands. So on the ordinary path no later tick holds both a dispatched row and an entry to observe it, and the guard before `MergePR` (stored fingerprint equal to the computed one and `dispatched` set) does not fire. It is reachable only when the row outlives its deletion, because `reaction_fingerprints` is persisted while pending entries are held in memory and re-created after a restart from recovered run metadata: a surviving dispatched row then meets a re-created entry and skips a merge that already completed.

### 11C.8 Adapter registration

Activation is via `reactions.auto_merge.provider` being non-empty in `WORKFLOW.md`, mirroring the `reactions.review_comments.provider` activation pattern documented in §11B.8. The `SCMAdapterConstructor` type and registry registration code are defined in §11B.8; the auto-merge path reuses the same constructor and registration mechanism without change.

### 11C.9 Token scope and preflight

The startup token-scope preflight is documented in full in §6.3. The auto-merge reaction requires two token scopes:

- `pull_requests:write`: always required for `MergePR`.
- `contents:write`: required when `reactions.auto_merge.delete_branch != false` (for `DeleteBranch`).

A classic `repo` scope satisfies both. Fine-grained PAT permission names are accepted.

Scope verification is best-effort: the preflight relies on `X-OAuth-Scopes` returned by `GET /rate_limit`. Classic personal access tokens populate that header; fine-grained PATs and GitHub App installation tokens do not. When the verifier returns no scope information (and no error), and when the adapter does not implement the scope-verifier interface, the preflight fails open: it logs WARN and proceeds. Genuine scope gaps for those token classes surface at runtime as `ErrSCMAuth` on the first `MergePR` attempt, which the orchestrator dispatches to the auth-class escalation path. See §6.3 for the full fail-open semantics.

Three failure postures apply:

- **Auth-class sticky**: a missing-scope failure (the verifier returns one or more missing scope names) sets `state.AutoMergePreflightFailed = true` permanently for the process lifetime. Every subsequent `reconcileAutoMerge` tick drops `merge`-kind pending entries that hold no standing escalation, with a WARN log.
- **Transport-class bounded retry**: a transport failure on the preflight call schedules one retry via `state.AutoMergePreflightRetryDueAt`. After one retry attempt, any further transport failure leaves the sticky flag set.
- **Fail-open (no scope information)**: the verifier returned no scopes and no missing entries (fine-grained PAT or GitHub App token), or the adapter does not implement scope verification. The orchestrator proceeds with auto-merge enabled and surfaces any real scope gap at the first `MergePR` call.

**Gitea caveat.** The Gitea adapter cannot reproduce this scope check: Gitea exposes no `/rate_limit` endpoint and returns no `X-OAuth-Scopes` header, so no startup request reveals a token's own scope. Its preflight substitutes a user-role gate for scope verification: at startup it reads the repository's `permissions.push` field for the configured token and fails the preflight when the token's user lacks repository write access. This is a check on the token user's role, not a verification of the token's own scope: a token whose user has write access still passes even when the token itself is scoped read-only. That gap surfaces only at runtime, when a `MergePR` or `DeleteBranch` call returns a 403 that the adapter enriches to name the missing `write:repository` scope explicitly. An operator satisfies both checks by granting the Gitea token's user repository write access and the token the `write:repository` scope.

**GitLab caveat.** The GitLab adapter verifies the credential's own scope record rather than a repository or organization signal: it reads the token-introspection endpoint at startup. GitLab has no contents-versus-pull-request scope split, so the single `api` scope is the one requirement covering the merge, the branch delete, and the label write. A granular token's scope report is opaque and takes the fail-open path, as do an empty scope list and an introspection response the adapter cannot read, and an instance whose introspection route is unavailable takes the same fail-open path rather than disabling auto-merge, because that route can answer 404 for a hidden or blocked path as readily as for a genuinely missing one. A classic token whose scope list omits `api` is a verified gap instead, and takes the auth-class sticky posture. A branch-protection refusal on this platform is reported at runtime as an auth-class failure rather than being caught at startup.

For the full preflight algorithm, see §6.3.

### 11C.10 Cross-kind isolation

`postAutoMergeSuccess`, `escalateAutoMergeFailure`, and the drop of an entry whose pull request reads merged MUST NOT call any function that modifies state owned by a different reaction kind. Specifically, these paths MUST NOT call:

- `CancelRetry`
- `DeleteRetryEntry`
- a clear that ranges over every pending entry, attempt counter, or `reaction_fingerprints` row an issue holds
- `delete state.Claimed[issue_id]`

A failed auto-merge does NOT invalidate parallel `ci` or `review` reaction continuations on the same issue, so the auto-merge cleanup path is scoped to the `merge` kind only.

### 11C.11 Relationship to the merge completion reaction

Both the auto-merge and merge-completion (§11G) reactions observe the merge state of the same pull request through `GetMergeability`, but they act on it independently and neither one performs the other's job. The already-merged disposition in §11C.3 and §11C.5 (a merge rejection the adapter confirmed by re-read to be an already-completed merge, treated as idempotent success) takes the actions of a merge this reaction performed, including the `auto_merge.merged` event and the branch delete when `delete_branch == true`; its cleanup touches only the `merge`-kind pending entry and fingerprint, and it never calls `TrackerAdapter.TransitionIssue`, so it never moves the tracker issue. Closing the tracker issue is the merge-completion reaction's exclusive responsibility, and it runs as an independent reconcile pass that reaches the same conclusion by observing `PRMergeStatus.Merged` on its own polling schedule, whether the merge that satisfied it was performed by this reaction, by a human, or by a forge automation rule.

