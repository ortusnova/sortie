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
- Hooks run inside the workspace directory, through the execution environment: on the host under `local`, in the issue's VM under `smolvm` (Section 15.9). A hook in a VM still runs code from the repository, such as a package install script, but that code then runs in the VM.
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

Two credentials of Sortie's own HTTP control surface (§13.7) are bounded on purpose. The operator's `server.control_token` never reaches an agent: every variable of the orchestrator's environment holding its value is removed at startup. Each work session instead receives `SORTIE_SESSION_TOKEN`, a token that authorizes only the per-issue control routes of its own issue and stops working when its run ends. A local agent can read that token, as it can read anything its wrapper holds, so the worst a leaked session token does is act on its own run, in ways the run's limits still bound and the run's state shows.

The `smolvm` execution environment (Section 15.9) moves this exposure off the host: the runtime, its configuration loader, and every helper it starts run in the issue's VM, with only the variables Sortie gives the VM.

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

`environment.kind: smolvm` (Section 9.7) is Sortie's built-in sandbox. Its goal: no code from the workspace or the agent runs on the host, and that includes `git` reading a repository the guest controls. The `local` kind keeps the host-execution model of the sections above.

**What runs in the VM.** The agent, its credential verification session, auxiliary agent commands, the four hooks, self-review verification commands, reaction triage, and every `git` command that inspects the workspace. The host only reads the output bytes of the inspection commands, with the existing caps. There is no fallback to the host when the VM cannot be prepared.

**No host git on a guest repository.** The repository lives on the VM's disk. When `environment.smolvm.repository` is set, git data crosses only as bundles piped through a guest command, outside the workspace. The host imports exactly one ref, `refs/heads/<issue branch>`, from a bundle into its own bare mirror, after checking that the bundle names nothing else and stays under `max_bundle_mib`. Every host git command on the mirror runs with hooks disabled, the filesystem monitor off, and every received object checked, so a planted hook, filter driver, or `.gitattributes` entry in an exported commit runs nothing on the host. A planted fsmonitor or filter can still run in the guest, where the agent can run anything anyway; at worst it falsifies its own handoff evidence. Reading `.git` objects through a shared mount is never done.

**Credentials.** Sortie pushes with the host's own git credentials (SSH agent, credential helper, deploy key); they never enter the VM, and the only ref Sortie pushes is the issue branch, with a lease, so a commit someone else pushed is never overwritten. Prefer a deploy key or a token scoped to the one repository. An agent's API key enters only as a smolvm credential binding: the guest holds a placeholder, and the proxy substitutes the value only on HTTPS requests to the bound hosts. A binding's `command` runs on Sortie's host, outside the VM, as Sortie's user and with Sortie's environment, so it is trusted configuration, like the workflow file that names it; its value is registered for redaction and never logged. `pass_env` copies a host variable into the VM in plain text, by explicit opt-in. An agent credential variable that is set on the host and neither bound nor passed fails the dispatch preflight; it is never copied silently. No other host variable reaches the VM: hooks and inspection commands there inherit nothing from Sortie's process, `SORTIE_*` variables included. `SORTIE_SESSION_TOKEN` and `SORTIE_API_URL` never reach the VM; a host prefix keeps them on the host. The generated MCP configuration carries no env block for Sortie's own server, but servers an operator adds through an agent block's `mcp_config` are merged as written and run in the VM, so any value in their env blocks is visible there.

**Egress.** A VM reaches no network host unless `allow_hosts` lists it, plus the hosts of its credential bindings, which Sortie always adds because a binding without an allowlist opens the whole network. With `repository` set, the git host needs no entry, so git is not an exfiltration path. Every listed host still is one: a package registry, or the model API itself. Denials are logged by smolvm (`smolvm machine egress-events --name <vm>`); the VM name is in `/api/v1/state`. smolvm enforces the allowlist on its virtio-net stack, the backend it picks for a VM with an allowlist, and not on TSI, so `net_backend` accepts only `virtio-net` and needs an allowlist or a credential: no setting gives a VM unfiltered egress.

**Host resources.** Sortie's own two: the issue's `.sortie` directory read-write and the issue's tool channel socket. No Sortie binary enters the guest. No system mounts, Docker socket, or published ports. SSH agent forwarding (`ssh_agent`), nested virtualization (`nested`), and host directories (`mounts`) are explicit opt-ins, each off by default. A pack does not carry its image's `USER`, so guest commands run as root unless `user` names the machine's user; the VM is the boundary either way, and a non-root user only narrows what the agent can change inside it. A mounted directory is readable by everything in the VM, and a read-write one is writable by it, so a workflow mounts data, never a credential; a credential stays a binding. A guest-planted link in a mount resolves in the guest and does not reach the host's files. A mount may not cover Sortie's own guest paths or lie under `workspace.root`. Validation does not refuse a `host` that contains `workspace.root` or Sortie's state directory, which would show the guest every issue's control files or the database, so a workflow mounts narrow directories.

**The shared control directory.** Files the agent writes there are read on the host through `workspacekit`, with links refused and sizes capped (Invariant 7). A guest can plant a link there; it is refused, not followed. The environment record there is agent-writable, so removal never follows a record to the host (Invariant 11). The trusted SCM record lives in `.sortie-host`, outside the shared directory, so an agent cannot point CI polling, reviews, or auto-merge at another pull request.

**The tool channel.** Any process in the VM can connect to the socket and to the guest port that serves it, but the tool server answers only requests that carry the session's bearer, which reaches the agent by name and dies with the session. The socket exists only while a session runs, serves only its own issue, and leads only to that session's tool server, which runs on the host with that issue's identity. The guest reaches no other host port: smolvm's egress policy covers neither the host's loopback nor its gateway address. The workflow file, the tracker credentials, and the database stay on the host.

**What isolation does not cover.** Everything the agent commits reaches the upstream issue branch, including changes to CI configuration or to the repository's own sandbox setup; branch protection on the base branch stays the merge gate. Data can leave through any allowed host. The shared control directory can fill the host disk up to the workspace root's filesystem; Sortie never reads past its caps. Stopping a session ends the agent at once, with no graceful signal. The VM boundary is smolvm's; Sortie refuses a smolvm release older than `1.32.4`, the release whose behavior was last measured, and accepts every later one without measuring it.
