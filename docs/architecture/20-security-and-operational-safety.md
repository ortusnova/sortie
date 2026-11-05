## 15. Security and Operational Safety

### 15.1 Trust Boundary Assumption

Each deployment defines its own trust boundary.

Operational safety requirements:

- Deployments should state clearly whether they are intended for trusted environments, more restrictive environments, or both.
- Deployments should state clearly whether they rely on auto-approved actions, operator approvals, stricter sandboxing, or some combination of those controls.
- Workspace isolation and path validation are important baseline controls, but they are not a substitute for whatever approval and sandbox policy a deployment chooses.
- The HTTP server has no authentication. A page served from another origin cannot change state through it: the server refuses such a state-changing request. DNS rebinding is outside this protection. Any other client that reaches the port can change state, so a deployment others can reach sits behind an authenticating reverse proxy or a VPN.

### 15.2 Filesystem Safety Requirements

Mandatory:

- Workspace path must remain under configured workspace root.
- Coding-agent cwd must be the per-issue workspace path for the current run.
- Workspace directory names must use sanitized identifiers.

Recommended additional hardening:

- Run under a dedicated OS user.
- Restrict workspace root permissions.
- Mount workspace root on a dedicated volume if possible.

### 15.3 Secret Handling

- Support `$VAR` indirection in workflow config.
- Validate presence of secrets without printing them.
- Every log record and every runtime-derived text Sortie stores or shows an operator has each secret value it holds replaced by a fixed marker. The values are those Sortie holds under a name that marks a credential (its environment variables and environment file, configuration keys, and the environment entries and headers of the tool servers it hands a runtime), the tracker API key, the credentials in a URL's userinfo, and notification endpoint URLs, which grant access to whoever holds them. A value too short to mask is named in a warning, never shown. Matching is exact, and every bound that cuts such text applies after masking. The one exception is the agent's stop statement, which Sortie reads only up to a byte bound before it can mask anything; there masking also removes a trailing fragment that is a prefix of a held value, so the cut cannot leave the start of one behind. A value Sortie never held, one it holds under a name no convention marks as a credential, or one its producer transforms before printing it, is outside what masking recognizes.
- The workflow shape the state API serves is an allowlist of settings that hold no credential. It carries no endpoint, key, command, hook or triage script, settings block, reaction extra key, or notification destination, and its configuration strings are served unmasked.
- A pending reaction row of the state API carries CI check names and outcome counts, never a log excerpt or a check's details link, because a log excerpt is not safe to expose through an unauthenticated API. Every runtime-derived string of the row (the issue's title, link, type, labels, and state, the repository, the pull request link, the head branch, the head commits, and each failing check name) is masked as above, in the snapshot rather than in the state it is copied from.
- The prices route serves a price-table address as its scheme, host, and path only, serves `price_table.replace_file` as the setting value (after `$VAR` resolution, before `~` expansion and path joining), and masks every error as the Health workflow check's error is masked, so it serves no credential and no path Sortie resolved.

### 15.4 Hook Script Safety

Workspace hooks are arbitrary shell scripts from `WORKFLOW.md`.

Implications:

- Hooks are fully trusted configuration.
- Hooks run inside the workspace directory, through the execution environment: on the host under `local`. Under `remote` they are refused (Section 15.9).
- Hook output should be truncated in logs.
- Hook timeouts are required to avoid hanging the orchestrator.

### 15.5 Harness Hardening Guidance

Running coding agents against repositories, issue trackers, and other inputs that may contain sensitive data or externally-controlled content can be dangerous. A permissive deployment can lead to data leaks, destructive mutations, or full machine compromise if the agent is induced to execute harmful commands or use overly-powerful integrations.

Deployments should explicitly evaluate their own risk profile and harden the execution harness where appropriate. Sortie does not mandate a single hardening posture, but deployments should not assume that tracker data, repository contents, prompt inputs, or tool arguments are fully trustworthy just because they originate inside a normal workflow.

Prompt injection risk: issue descriptions, comments, and labels are untrusted input that flows directly into agent prompts. Workflow prompts should include defensive instructions to reduce the risk of injected content manipulating agent behavior.

Possible hardening measures include:

- Tightening agent approval and sandbox settings instead of running with a maximally permissive configuration.
- Adding external isolation layers such as OS/container/VM sandboxing, network restrictions, or separate credentials beyond the built-in agent policy controls.
- Using `tracker.query_filter` to restrict which issues are eligible for dispatch (e.g., by label, component, epic, or other tracker-native criteria) so untrusted or out-of-scope tasks do not automatically reach the agent.
- Scoping the `tracker_api` tool (Section 10.4.5) so it can only read or mutate data inside the intended project scope, rather than exposing general tracker access.
- Reducing the set of registered tools, credentials, filesystem paths, and network destinations available to the agent to the minimum needed for the workflow.
- Bounding what an unattended deployment spends with the per-issue ceilings (`agent.max_sessions`, `agent.max_tokens`, `agent.max_cost_usd`) and the instance-wide ceilings on a UTC calendar day, a UTC week from Monday, and a UTC calendar month (`agent.max_daily_cost_usd`, `agent.max_weekly_cost_usd`, `agent.max_monthly_cost_usd`). An instance ceiling starts no new agent session while its window's estimated spend stands at the ceiling, and it stops no running session, so a window's final spend can pass the ceiling by what running sessions spend. The figure is Sortie's estimate, not the provider's bill.

The correct controls are deployment-specific, but deployments should document them clearly and treat harness hardening as part of the core safety model rather than an optional afterthought.

### 15.6 Workspace Content as Agent Configuration

Some agent runtimes read configuration out of the working directory they are handed, and start helper processes from what they find there. Sortie creates a workspace, checks repository content into it, and gives that directory to the agent as its working directory, so repository content reaches the runtime's configuration loader. Content that arrives with a checkout can therefore shape how the agent runs, and on a runtime that launches processes from configuration it can execute on the host under the account Sortie runs as.

Processes an agent runtime starts on its own behalf are not confined by that agent's sandbox setting. The sandbox governs commands the agent executes; it does not govern the transports and helper processes the runtime launches to serve the session. Nor does it govern whether workspace configuration is read at all. A runtime that gates that reading on a recorded trust decision consults the sandbox only when deciding whether to record trust for a path it has not seen before, so a restrictive sandbox prevents the decision rather than the loading. On a path that has never been trusted it therefore does protect: no decision is recorded, so nothing project scoped is read and no helper declared there is started. A path already carrying that decision keeps loading its configuration, and keeps starting the processes that configuration declares, whatever the sandbox says on the run that reaches it. The approval policy makes no difference to any of this. Tightening the sandbox setting therefore does not close the exposure, which is the limit of the hardening guidance above.

Sortie derives a workspace path from the issue identifier, so runs for the same unit of work share a path. A trust decision recorded against that path outlives both the run and the workspace content that earned it, so the first permissive run for an issue arms every later run at the same path.

The workspace Sortie creates and hands over is the boundary that matters, and two operator-facing controls act on it. Give the agent runtime a configuration home scoped to the run, so whatever it records while running dies with the run instead of accumulating in the operator's own configuration; this is demonstrated workable rather than theoretical. Control what lands in the checkout, because the exposure is bounded by who can place a file in the checked-out tree: a workflow that checks out contributor-supplied refs widens it well beyond one that builds only the default branch.

Sortie launches a local agent with the orchestrator's environment, so any credential present there is readable by an agent running with approvals disabled in a write-capable sandbox. Whether a tracker credential is present depends on how it is supplied. A value named indirectly through an environment variable, or supplied by an environment override, sits in the orchestrator's environment and is inherited. A value written literally into workflow configuration does not, and reaches the tracker client without passing through the environment, although it then sits in the configuration file instead.

Dispatching a run to a remote host bounds this differently. Only the variables the operator names plus the credential variables the agent kind declares, less the variables the operator disallows, cross to the remote host, delivered outside any process argument list, so the remote agent inherits that set rather than the orchestrator's environment, and the exposure is still the size of that set.

The sandbox does not help in either case. It governs filesystem and network reach, not what a process reads from the environment it was started with.

Two credentials of Sortie's own HTTP control surface (§13.7) are bounded on purpose. The operator's `server.control_token` never reaches an agent: every variable of the orchestrator's environment holding its value is removed at startup. Each work session instead receives `SORTIE_SESSION_TOKEN`, a token that authorizes only the per-issue control routes of its own issue and stops working when its run ends. A local agent can read that token, as it can read anything its wrapper holds, so the worst a leaked session token does is act on its own run, in ways the run's limits still bound and the run's state shows. A third, `server.supervisor_token`, is held by a supervising agent outside the runs and is removed from the orchestrator's environment as the control token is. The one agent Sortie gives it is its own supervisor session (§5.3.12), in that session's prompt and in `SORTIE_SUPERVISOR_TOKEN`; the session runs on Sortie's host in its own directory, never in a story's workspace, so no story session can read it. It authorizes only the supervisor's actions (answer, release, capped session grants, a merge record the forge confirms, a fix run, a draft, a merge whose review and CI passed and that is not a `review: ui` story, an open issue, a live-proof request, a merge-and-prove of a story only a deploy proves, which marks a draft ready for review and reverts the merge when it is not proven in time, a fresh criteria review of a story's head, a question for a person that holds the stories it names), never a pause of itself, a spec change, an approval, a credential or key, or a rewrite of a pushed commit, and Sortie stores every request it makes, refused ones included, with its evidence and result.

The `remote` execution environment (Section 15.9) moves this exposure off the host: the runtime, its configuration loader, and every helper it starts run in the job's machine on a worker.

### 15.7 Outbound Requests on Sortie's Own Account

Every destination Sortie reaches belongs to an integration the workflow configures (the tracker, the forge and CI provider, the notification backends, and the agent runtimes) except one: the price-table download (Section 13.5). It is on by default: a default install contacts `price_table.fetch_url` at startup and once every 24 hours. A download that fails is tried again after 1 minute, and each further failure doubles the wait up to 1 hour, so a deployment whose network returns has a table within the hour; while tries keep failing, a download sends at most 29 requests in its first 24 hours and 24 a day after. Its default address, the published LiteLLM price file, is the only endpoint literal compiled into the binary that does not belong to a configured integration.

- An operator in an audited or network-restricted environment turns the download of the shared table off with `price_table.fetch: false` in WORKFLOW.md, or with `SORTIE_PRICE_TABLE_FETCH=false` in the process environment, which overrides the workflow file. With it off and no `price_table.replace_url`, Sortie makes no request on its own account, and runs are priced from `token_rates` and the embedded table; a table kept from an earlier download never prices a run while the download is off. The download of the shared table runs whenever `price_table.fetch` is on, whether or not an operator table is configured. A download of an operator table happens only when the operator names one with `price_table.replace_url`, which is that operator's explicit choice of destination and is not gated by `price_table.fetch`.
- The request is a download of a public file, not a reporting feature: it sends no run data, no configuration, and no operator-authored string, in its address, its headers, or anywhere else, beyond the address the operator wrote into `price_table.fetch_url` or `price_table.replace_url`.
- A failed or unanswered download never blocks or fails startup, dispatch, or a workflow reload; it is logged, and a deployment without network access keeps pricing from the last table that passed validation, stored beside the database, or from the embedded table.
- Sortie checks a downloaded table's shape, not its truth: it authenticates the publishing host through HTTPS for an `https` address and does not authenticate the content. A redirect from an `https` address to a target that is not `https` fails the try, so the default address cannot be downgraded; an `http` address is requested as written. A table that parses with false but plausible rates is accepted, and because money ceilings enforce on the recorded estimate, a forged lower rate lets spend pass a ceiling before it holds. An operator who needs reviewed prices points `price_table.fetch_url` at a pinned revision or an internal mirror, names a table of their own with `price_table.replace_file`, or turns the download off.
- A log line about a download carries the address's scheme, host, and path only, never its userinfo or query, and the table kept beside the database records the address as a digest, so a credential the operator writes into an address reaches neither.
- It goes through the standard proxy settings, `HTTPS_PROXY`, `HTTP_PROXY`, and `NO_PROXY`, like every other client Sortie builds.

### 15.8 Control API Exposure

The state-changing HTTP endpoints accept requests without a token only while no control token is set, and Sortie then refuses any bind address that is not a loopback address. A deployment that exposes the server, a container bound to `0.0.0.0` among them, sets `server.control_token`. Read-only endpoints keep no authentication: put the server behind a network boundary or an authenticating proxy when the dashboard, the runtime snapshot, or the metrics must not be readable.

Which runtimes read workspace configuration, which files they key on, and how any of this differs between their releases are adapter concerns and belong in the adapter notes.

### 15.9 Execution Environment Isolation

`environment.kind: remote` (Section 9.7) is Sortie's built-in isolation. Its goal: no code from the workspace or the agent runs on Sortie's host, and that includes `git` reading a repository the agent controls. The agent runs in a job's machine on a worker that dials Sortie; the worker's isolation backend is the machine boundary. The `local` kind keeps the host-execution model of the sections above. The `smolvm` kind, which ran a microVM per issue on Sortie's own host, was removed.

**What runs on the worker.** The agent alone. The four hooks, self-review verification commands, reaction triage, and the inspection of the handoff evidence are refused under `remote`, and the credential verification session does not run. There is no fallback to Sortie's host.

**No host git on an agent's repository.** The repository lives in the job's machine. Git data crosses only as bundles on the `bundles` flow: the seed Sortie builds from its mirror, and the issue branch the worker returns. Sortie imports exactly one ref, `refs/heads/<issue branch>`, from the returned bundle into its own bare mirror, after checking that the bundle names nothing else, that the branch is the seed's commit or on top of it, and that it stays under `max_bundle_mib`. Every host git command on the mirror runs with hooks disabled, the filesystem monitor off, and every received object checked, so a planted hook, filter driver, or `.gitattributes` entry in an exported commit runs nothing on the host.

**Credentials.** Sortie pushes with the host's own git credentials (SSH agent, credential helper, deploy key); they never reach a worker, and the only ref Sortie pushes is the issue branch, with a lease, so a commit someone else pushed is never overwritten. Prefer a deploy key or a token scoped to the one repository. An offer, its run descriptor and the seed carry no git, forge or tracker credential, no repository URL, and no session token. An agent's API key reaches the machine only as a placeholder that the worker's egress proxy replaces on requests to the credential's hosts; Sortie never reads the value. A flow's job token is the only secret a worker can present to Sortie: it is bound to one job, attempt and flow, Sortie keeps only its digest, and it is void once the attempt ends.

**The tool channel.** The agent reaches Sortie's tool server only through the `mcp` flow, admitted by that attempt's live `mcp` token, and Sortie forwards the request to the tool server of that attempt's session with the session's bearer. The tool server runs on Sortie's host with that issue's identity, and the workflow file, the tracker credentials, and the database stay there.

**The trusted SCM record.** It lives in `.sortie-host`, written by Sortie after a push, so an agent cannot point CI polling, reviews, or auto-merge at another pull request. The agent's `.sortie/pr.json` comes back beside the bundle, capped, and is read as untrusted text.

**The egress policy is the base branch's.** A job's offer carries what its machine may reach: the workflow's `environment.remote.egress` for the agent harness, the hosts of the credential bindings, and the repository's `.sortie/egress.yaml`, which Sortie reads from its mirror at the tip of the base branch and never from the issue branch or a workspace. An agent that adds a host to the file on its issue branch therefore widens no offer, its own included; the host reaches an offer only once a reviewed merge puts it on the base branch, so branch protection on the base branch gates the policy as it gates the code. An invalid file holds the repository's jobs rather than offering them with a guessed policy.

**What isolation does not cover.** Everything the agent commits reaches the upstream issue branch, including changes to CI configuration or to the repository's own sandbox setup; branch protection on the base branch stays the merge gate. Data can leave through any host the job's egress policy allows. The machine boundary and the enforcement of the egress policy are the worker's.
