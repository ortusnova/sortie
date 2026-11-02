# smolvm environment guide

The `smolvm` execution environment runs each issue in its own microVM. The agent, the hooks, the verification commands, reaction triage, and Sortie's `git` inspection all run in the VM. The VM sees only the hosts you allow, sees API keys only as placeholders, and has no git credentials. With a `repository`, Sortie itself clones, pushes, and opens the pull request.

This guide is for operators. For every setting and error, see [the `environment` block](workflow-reference.md#212-environment--execution-environment) in the workflow reference. For the design, see [Execution Environments](architecture/09-workspace-management-and-safety.md#97-execution-environments) and [Execution Environment Isolation](architecture/20-security-and-operational-safety.md#159-execution-environment-isolation).

## Requirements

- A Linux host with KVM. The VM runs a copy of Sortie's own binary, so Sortie refuses `smolvm` on any other OS.
- smolvm 1.32.4 or later. Sortie runs `smolvm --version` at startup and on every reload that selects the kind, and refuses an older release or a version it cannot parse. 1.32.4 is the release smolvm's behavior was last measured on; later releases are accepted without an upper bound.
- `git` on the host, and git credentials that can fetch and push the repository, when you use `repository`.
- podman or docker, and `smol-builder`, to build packs. They are not needed on the host that runs Sortie if you build packs elsewhere.
- `environment.kind: smolvm` cannot be combined with `worker.ssh_hosts`.

## Install smolvm

Download the official install script, read it, then run it with the pinned version:

```sh
curl -sSL https://smolmachines.com/install.sh -o install.sh
less install.sh
bash install.sh --version 1.32.4 --no-modify-path
smolvm --version        # smolvm 1.32.4
```

As a normal user, the script installs into `~/.smolvm` and links `~/.local/bin/smolvm`. As root on Linux it installs into `/opt/smolvm`. It checks the archive against the release's `checksums.sha256`, and only warns when that file is missing, so look for `Checksum verified` in its output. `--no-modify-path` leaves your shell profile alone; make sure `smolvm` is on the `PATH` of the user Sortie runs as, or set `environment.smolvm.binary` to its full path.

Run Sortie as the same user that owns the smolvm installation and the pack cache.

## Build a pack

A pack is a `.smolmachine` file: the VM image every issue's VM is created from. Build it with `smol-builder`, a separate static Go binary (`make build` in its repository; its README documents the layer library and every pin). It reads `smol-builder.toml` at the root of the project's repository:

```toml
layers = ["go", "node"]                # library layers: go, node, python, rust
project_layer = "ci/agent.Dockerfile"  # optional, applied last
```

The `base` layer is always applied. It is Debian with `git`, a C toolchain, `curl`, `jq`, `ripgrep`, and opencode with a policy-only configuration, and its working directory is `/workspace`. A project layer starts with `ARG BASE` and `FROM ${BASE}` and installs the project's own system packages:

```dockerfile
ARG BASE
FROM ${BASE}
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends libpq-dev \
 && rm -rf /var/lib/apt/lists/*
```

Build it:

```sh
smol-builder build --repo ~/src/app --output /var/lib/smol-builder/app.smolmachine > report.json
jq -r .pack_sha256 report.json
```

The output is replaced atomically, and a failed build leaves the old pack in place. No layer holds a credential: credentials are bound to each VM when it is created, never baked into the pack.

A pack does not carry its image's `USER`. A project layer that ends in `USER builder` still gets VMs that run every command as root. To run as that user, set `environment.smolvm.user: builder` (see [The machine's user](workflow-reference.md#the-machines-user)). The user needs write access to `/workspace` and to its home, and should have the uid Sortie runs as on the host, since the shared `.sortie` directory shows the guest that owner:

```dockerfile
RUN useradd --create-home --uid 1000 builder
USER builder
```

Point `environment.smolvm.pack` at the file. Sortie records the pack's SHA-256 in each new VM's `sortie.pack` label and in the `environment_image_sha256` column of every run. Rebuilding the pack affects only VMs created afterwards. An existing issue keeps the VM, and the pack, it was created with. To move an issue onto the new pack, remove its workspace; Sortie creates a new VM at its next dispatch.

## An example workflow

This workflow runs `opencode acp` in a VM built from a Go pack. Sortie clones `org/app` into the VM, pushes the agent's commits, and opens the pull request. Replace the pack path, the repository, and the tracker with your own.

```yaml
---
tracker:
  kind: file
  active_states: [To Do, In Progress]
  terminal_states: [Done]
file:
  path: issues.json

workspace:
  root: $SORTIE_WORKSPACE_ROOT

hooks:
  after_create: |
    go mod download

agent:
  kind: agent-client-protocol
  command: opencode acp
  max_turns: 10

environment:
  kind: smolvm
  smolvm:
    pack: /var/lib/smol-builder/app.smolmachine
    cpus: 4
    memory_mib: 8192
    storage_gib: 20
    allow_hosts:
      - openrouter.ai
      - models.opencode.ai
      - registry.npmjs.org
      - proxy.golang.org
      - sum.golang.org
    credentials:
      - name: openrouter
        env: OPENROUTER_API_KEY
        hosts: [openrouter.ai]
    repository:
      url: git@github.com:org/app.git
      base: main
      provider: github
      project: org/app

github:
  api_key: $SORTIE_GITHUB_TOKEN
---
You are working on {{ .issue.identifier }}: {{ .issue.title }}.

{{ if .issue.description }}{{ .issue.description }}{{ end }}

The repository is checked out in the current directory, on the branch for
this issue. Make the change, run the tests, and commit your work with git.
Do not push and do not open a pull request: Sortie does both after the run.
Only committed work leaves this machine.

Before you finish, write `.sortie/pr.json` with a JSON object holding a
short `title` and a `body` that explains the change.
```

`SORTIE_WORKSPACE_ROOT`, `OPENROUTER_API_KEY`, and `SORTIE_GITHUB_TOKEN` are set in Sortie's environment. The first reaches the VM only as a placeholder; the second never reaches it. The base pack's opencode configuration selects an OpenRouter model, so no other setting is needed for it.

Check the workflow before you start Sortie:

```sh
sortie validate WORKFLOW.md
```

`sortie validate` runs the same smolvm version check as startup, and reports an agent credential that the VM would not receive.

## Agents

### Which agent kinds work

| Agent kind | Under smolvm |
| --- | --- |
| `agent-client-protocol` with `opencode acp` | Supported. Verified end to end in a VM. |
| `agent-client-protocol` with `claude-agent-acp` | Supported. Verified with one turn through OpenRouter's Anthropic-compatible endpoint. |
| `claude-code` | Supported. Verified end to end in a VM (Claude Code 2.1.308, Claude subscription OAuth token). Needs `IS_SANDBOX` (below). |
| `codex` | Supported with `codex.thread_sandbox: dangerFullAccess`. Verified end to end in a VM (codex 0.154.0 through OpenRouter). Under the default `workspaceWrite`, codex cannot write `.sortie/` (below). |
| `copilot-cli` | Not usable with a credential binding: Copilot CLI refuses the placeholder as an "Unsupported token type" before it sends a request. Passing a real token through `pass_env` puts it in the VM in plain text; that path is not verified. |
| `pi` | Not verified. |

The agent's binary must be in the pack: the VM cannot see the host's. The `base` layer ships only opencode. claude-agent-acp, Claude Code, codex, and Copilot CLI need Node, from the `node` layer, and an install in your project layer, for example `RUN npm i -g @anthropic-ai/claude-code@<version> @openai/codex@<version>`.

The agent runs as root in the VM. Claude Code refuses `--dangerously-skip-permissions` as root unless `IS_SANDBOX=1` is set: export it in Sortie's environment and list it in `pass_env`.

codex's `workspaceWrite` sandbox allows writes only under the workspace. In the VM, `.sortie` is a link to the control directory outside it, so codex cannot write `.sortie/status`, `.sortie/review_verdict.json`, or `.sortie/pr.json`. Set `codex.thread_sandbox: dangerFullAccess`; the VM is the sandbox. codex 0.154 refuses `externalSandbox` in `thread/start`.

### `allow_hosts` for each agent

The VM reaches no host that is not listed. A credential binding's hosts are added for you.

| Agent | Hosts it needs |
| --- | --- |
| opencode through OpenRouter | `openrouter.ai` (or your provider's API host), `models.opencode.ai`, `registry.npmjs.org` |
| claude-agent-acp | The API host of its Anthropic-compatible endpoint: `api.anthropic.com`, or `openrouter.ai` for OpenRouter |
| claude-code | `api.anthropic.com` |
| codex through OpenRouter | `openrouter.ai`. codex also calls `api.github.com`, `github.com`, and `chatgpt.com`; the turn works with all of them denied. |

opencode needs `models.opencode.ai`. Without it, opencode silently falls back to the model `opencode/big-pickle`, and every call fails. It fetches provider packages from `registry.npmjs.org`.

Add the hosts your hooks and tests need, such as `proxy.golang.org` and `sum.golang.org` for `go mod download`, or `registry.npmjs.org` for `npm ci`. Do not add the git host when you use `repository`: Sortie fetches and pushes for the VM.

### Credential bindings

Bind every API key the agent reads:

```yaml
credentials:
  - name: openrouter
    env: OPENROUTER_API_KEY
    hosts: [openrouter.ai]
```

- In the VM, `OPENROUTER_API_KEY` holds a placeholder value that smolvm generates. The key itself never enters the VM.
- Never put a placeholder value into anything an agent reads: issue text, comments, or prompt files. An agent sends what it reads to its model, smolvm then rejects every request that carries the placeholder, and the agent's session fails with `credential_unverified`.
- smolvm reads the value from Sortie's environment each time it starts the VM. Set the variable before Sortie starts; Sortie logs a warning for a binding whose variable is not set.
- For a token that a login refreshes, name its source instead: `file: <path>` or `command: <shell command>`. Sortie reads it on its host each time it starts the VM for a run, so the next run gets the refreshed token without a restart of Sortie. A source that yields no value fails the run before its VM starts. See [A credential read at each launch](workflow-reference.md#a-credential-read-at-each-launch).
- The placeholder is replaced only on HTTPS requests to the listed hosts. A request that carries it in the query string is refused.
- On those hosts, requests are HTTP/1.1 only, cannot upgrade to a WebSocket, and carry at most 8 MiB of body.
- The binding's hosts are added to the allowlist.
- `header` names a request header that smolvm sets to the key, for an API that reads it from a header such as `X-Api-Key` instead of `Authorization: Bearer`. smolvm sets it on every HTTPS request to the binding's hosts, even when the agent does not send it, and replaces any value the agent sends in it. It is never set on other hosts or on plain HTTP. The placeholder is still replaced in the request's other headers.

claude-agent-acp through OpenRouter needs three variables. Bind the token, and set the other two in the agent's command:

```yaml
agent:
  kind: agent-client-protocol
  command: [env, ANTHROPIC_BASE_URL=https://openrouter.ai/api, ANTHROPIC_API_KEY=, claude-agent-acp]

environment:
  kind: smolvm
  smolvm:
    credentials:
      - {name: openrouter, env: ANTHROPIC_AUTH_TOKEN, hosts: [openrouter.ai]}
```

Export `ANTHROPIC_AUTH_TOKEN` with your OpenRouter key in Sortie's environment. `ANTHROPIC_API_KEY` must be empty in the VM.

Claude Code with a Claude subscription reads an OAuth token from `CLAUDE_CODE_OAUTH_TOKEN`; bind it to `api.anthropic.com`:

```yaml
environment:
  kind: smolvm
  smolvm:
    pass_env: [IS_SANDBOX]
    credentials:
      - {name: anthropic, env: CLAUDE_CODE_OAUTH_TOKEN, hosts: [api.anthropic.com]}
```

codex through OpenRouter takes its provider from `-c` options. Put them after `app-server`: options before the subcommand do not reach the app-server's threads, which then use the default OpenAI provider.

```yaml
agent:
  kind: codex
  command:
    - codex
    - app-server
    - -c
    - model_provider="openrouter"
    - -c
    - model_providers.openrouter={name="OpenRouter", base_url="https://openrouter.ai/api/v1", env_key="OPENROUTER_API_KEY", wire_api="responses", supports_websockets=false}
    - -c
    - model="openai/gpt-5.1-codex-mini"

codex:
  thread_sandbox: dangerFullAccess

environment:
  kind: smolvm
  smolvm:
    credentials:
      - {name: openrouter, env: OPENROUTER_API_KEY, hosts: [openrouter.ai]}
```

When an agent kind declares a credential variable, such as `ANTHROPIC_API_KEY` for `claude-code`, and that variable is set in Sortie's environment, bind it or list it in `pass_env`, or unset it. Otherwise the dispatch preflight fails under `environment.agent_credential`. `pass_env` copies the value into the VM in plain text; use it only for values that may live there.

### Stopping a session

smolvm cannot send a termination signal into the VM. When Sortie stops a session, the agent ends at once: `agent.stop_grace_ms` gives it no time. The response to the turn in flight, and the result of a running tool call, are lost. The agent's own session state stays on the VM's disk. With opencode, a resumed session shows the interrupted tool call, and the agent checks the state and carries on.

### Sortie's tools

The agent in the VM keeps Sortie's tools, such as `sortie_status`. Sortie runs its tool server on the host, over HTTP, for each agent session, and hands the agent `http://127.0.0.1:47800/mcp` with a bearer that is valid only for that session. In the VM, a `socat` listener on that port carries each connection through the issue's socket to the host. The tracker credentials and the workflow file stay on the host, and the VM holds no Sortie binary. The pack must contain `socat`; smol-builder's `base` layer ships it, and a pack without it fails preparation with an error that names it.

### A broker on the host

To watch and steer sessions with `acp-broker`, put it in `host_prefix`, not in `agent.command`:

```yaml
environment:
  kind: smolvm
  host_prefix: [acp-broker, -http, 127.0.0.1:7700, -viewer-token-file, /etc/sortie/viewer-token, --]
```

The broker runs on the host and keeps the session token. Only the agent runs in the VM. See the [acp-broker operator guide](https://github.com/ortusnova/agentic-orchestration/blob/main/acp-broker/docs/guide.md).

## The repository

With `repository` set, only Sortie talks to the git host.

```yaml
repository:
  url: git@github.com:org/app.git
  base: main
  branch_prefix: sortie/     # default
  provider: github           # optional: open the pull request
  project: org/app           # required with provider
  max_bundle_mib: 512        # default
```

To let issues run in several repositories, list them under `repositories`, each with a `name`, and name the default in `default_repository`, if any. An issue runs in the repository its `sortie-repository:<name>` label names, which plan sync keeps from the plan, and its workspace stays with the repository it was seeded from. Sortie dispatches only the issues of the listed repositories that are open to dispatch; an entry with `dispatch: false` stays listed for plan sync while another Sortie on the same tracker project runs its issues. See [Several repositories](workflow-reference.md#several-repositories).

**The mirror.** Sortie keeps a bare mirror of each `url` under its state directory, at `<state dir>/repos/<hash>.git`, where `<state dir>` is the directory that holds the database. Only Sortie writes it, with hooks and the filesystem monitor off and every object checked. Git data moves between the mirror and the VM only as bundles, and Sortie never runs `git` on the repository inside the VM.

**Host git credentials.** Sortie fetches and pushes with the host's own git: the SSH agent, a credential helper, or an SSH deploy key configured for the user Sortie runs as. They never enter the VM. Check them as that user before you start Sortie:

```sh
git ls-remote git@github.com:org/app.git HEAD
```

Prefer a deploy key, or a token scoped to the one repository, with write access: Sortie pushes only the issue branch, but the credential can reach whatever it is scoped to.

**The issue branch** is `branch_prefix` followed by the workspace key: `sortie/ENG-42`.

**A new VM** is seeded before `after_create`: Sortie fetches the upstream, clones `base` into `/workspace`, and checks out the issue branch, at the upstream branch when it exists, otherwise new at `origin/<base>`. `origin` points at `url`, but the VM cannot reach it.

**Before each later run** Sortie fetches the upstream again and updates the VM's `origin/<base>` and `origin/<issue branch>`. The agent's branch and uncommitted changes are left alone. So the agent can rebase on a newer base, and commits a person pushed to the issue branch show up as `origin/<issue branch>`.

**After each run** that ended normally, and after `after_run`, Sortie exports the issue branch:

- Only committed work leaves the VM. Uncommitted changes stay there for the next run, so tell the agent to commit.
- The bundle must name only the issue branch, and be at most `max_bundle_mib`. It carries only the commits the mirror lacks, so a large repository exports small runs. smolvm caps a command's output at about 11 MiB, which bounds a run's new commits whatever `max_bundle_mib` says.
- Nothing new, or no issue branch in the VM, means no push.
- Sortie pushes with a lease. It replaces its own earlier push, so the agent may rebase. It never overwrites a commit someone else pushed: the run fails with `branch moved upstream`, and the next run shows the agent that commit.
- With `provider`, Sortie then finds the open pull request for the branch, or opens one against `base`. The agent can suggest the title and body of a new pull request in `.sortie/pr.json` (`{"title": "...", "body": "..."}`). Without that file, the title is `<identifier>: <issue title>`.
- A failure in any of these steps fails the run. The issue keeps its tracker state, the VM keeps the committed work, and the run is retried.

`provider` uses the SCM adapter settings of the top-level block of the same name, such as `github:` with `api_key`. It must be the same provider as every active SCM reaction. The token needs the right to open pull requests: the `repo` scope or "Pull requests: write" on GitHub, `api` and the Developer role on GitLab, `write:repository` on Gitea.

Sortie records the push and the pull request on the host, in `<workspace>/.sortie-host/scm.json`. CI, review, and auto-merge reactions read that record, not the agent's `.sortie/scm.json`. Under plan control the same mirror confirms a `no-change-needed` claim that cites a commit: the commit counts only when it is on the base branch, and git never runs in the workspace. Without `environment.smolvm.repository`, such claims stay unverified. A repository whose branch protection requires an approving review cannot auto-merge plan stories with the `criteria` review gate; use `review: human` there.

### What `after_create` should do now

With `repository`, the code is already in `/workspace` when `after_create` runs. So:

- Install dependencies: `go mod download`, `npm ci`, `uv sync`. List their registries in `allow_hosts`.
- Do not clone, fetch, push, or create branches. The VM has no route to the git host, and Sortie does all of it.
- Remove push and pull-request steps from `after_run`, `before_remove`, and the prompt.

Every hook runs in the VM, in `/workspace`, with `SORTIE_ENVIRONMENT=smolvm`. A hook inherits none of Sortie's own variables: list a variable it needs in `pass_env`.

Without `repository`, Sortie neither seeds nor exports. `after_create` must then get the code itself, from inside the VM, which needs the git host in `allow_hosts` and a credential in the VM.

## Nested VMs, the network backend, and host mounts

A workflow whose agent starts VMs of its own, or needs data from the host, opts in per field. None of them is on by default, and a workflow that sets none of them keeps its VMs as they were. See [Host inputs](workflow-reference.md#host-inputs) for every rule.

```yaml
environment:
  kind: smolvm
  smolvm:
    pack: ~/.local/share/smol/homelab.smolmachine
    nested: true                     # the guest gets /dev/kvm
    net_backend: virtio-net          # needs allow_hosts or credentials
    allow_hosts: [ghcr.io]
    mounts:
      - {host: ~/.cache/homelab-sources, guest: /home/agent/.cache/homelab-sources, read_only: true}
      - {host: ~/.local/state/sortie-recorder/{workspace}, guest: /var/recorder}
```

- **`nested`** needs nested KVM on the host: `cat /sys/module/kvm_intel/parameters/nested` (or `kvm_amd`) prints `Y` or `1`.
- **`net_backend`** accepts only `virtio-net`. smolvm filters egress on it by `allow_hosts` and the credential hosts, as it does for any VM with an allowlist. `tsi` is refused, because smolvm cannot keep an allowlist on it.
- **`mounts`** mount host directories over virtiofs. A shared `host` must exist when the workflow loads. Mount data, never a credential: everything in the VM can read a mount. Files the guest writes in a mount belong to Sortie's user on the host.
- **A `{workspace}` mount** gives each issue its own directory, for example `~/.local/state/sortie-recorder/ENG-42`. Sortie creates it, mode `0700`, before the VM, and never removes it, so it keeps what the guest wrote after the VM crashed or the workspace was removed. `/api/v1/state` lists it under `environment.kept_dirs`. Remove old directories yourself.

```sh
curl -s http://127.0.0.1:7678/api/v1/state | jq '.running[] | {issue_identifier, kept: .environment.kept_dirs}'
```

## Operating it

**Find an issue's VM.** Each issue's VM is named `sortie-<workspace key>`, for example `sortie-ENG-42`. `/api/v1/state` and `/api/v1/<identifier>` show it in an `environment` object:

```sh
curl -s http://127.0.0.1:7678/api/v1/state | jq '.running[] | {issue_identifier, environment}'
smolvm machine ls
smolvm machine ls --json | jq '.[] | select(.labels["sortie.managed"] == "1")'
```

A VM runs while its issue's worker runs, and is stopped between runs. It is deleted with the workspace, when the issue reaches a terminal state or the retention sweep removes it. At startup and on each periodic sweep, Sortie deletes the VMs it created for this workflow whose workspace directory is gone. It never touches a VM without its `sortie.managed` label.

**See what the VM was refused.** smolvm logs every connection the allowlist denied:

```sh
smolvm machine egress-events --name sortie-ENG-42
```

A host that a hook or the agent needs, and that shows up here, belongs in `allow_hosts`. Hosts an agent calls on its own, such as codex calling `chatgpt.com`, can stay denied.

**Keep the pack cache in check.** The first VM created from a new pack extracts it into smolvm's cache, `~/.cache/smolvm`, which grows by hundreds of MiB per pack and reached 9.8 GiB in testing. Every rebuilt pack adds an entry. Prune it:

```sh
smolvm pack prune --dry-run   # list what would go
smolvm pack prune --keep 5    # keep at most 5 unused entries
```

Entries that a VM still uses are always kept.

**Host state.** Sortie keeps its smolvm state next to the database: `env/<key>/` holds each issue's tool socket while an agent session runs, and `repos/` the mirrors. An `env-bin/` directory left by an earlier version is unused and can be removed. Do not edit them while Sortie runs. A smolvm command addressed to a machine that does not exist leaves an empty directory and an empty lock file for its name under smolvm's cache, `~/.cache/smolvm/vms` (or `$XDG_CACHE_HOME/smolvm/vms`); when Sortie finds an issue's VM already gone on release or removal, it removes those two entries, and only when they are empty. Each workspace directory under `workspace.root` holds only `.sortie` and `.sortie-host`; the code is in the VM.

**Do not manage Sortie's VMs by hand.** Deleting a VM discards the workspace on its disk; the next dispatch creates a new VM and runs `after_create` again. To start an issue over, remove its workspace directory instead, and the sweep deletes the VM.

**Upgrading smolvm.** Sortie accepts 1.32.4 and every later release, and refuses to start with an older one. Install the latest release with the install script's `--version`, then rebuild your packs with it.

## Rolling back

1. Set `environment.kind: local`, or remove the `environment` block.
2. Restore the clone in `after_create`, and the push and pull-request steps the agent or the hooks did before.
3. Restore the agent's own credentials on the host, if it needs them there.

The reload applies the change. Each existing workspace was created by `smolvm`, so Sortie removes it, with its VM, at the issue's next dispatch, and creates it again on the host, running `after_create`. The sweep deletes the VMs of issues that are not dispatched again. Branches and pull requests Sortie created stay upstream; uncommitted work in a VM is lost with it.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `environment.smolvm.binary: cannot read the version of smolvm binary` | smolvm is not on Sortie's `PATH`. Set `environment.smolvm.binary` to its full path. |
| `smolvm version 1.x.y is older than the minimum supported version 1.32.4` | Install 1.32.4 or later with the install script's `--version`. |
| `environment.agent_credential: agent kind ... reads the credential variable ...` | Bind that variable in `credentials`, list it in `pass_env`, or unset it in Sortie's environment. |
| `credential "<name>": its source (...) yielded no value: ...` | The binding's `file` or `command` gave no value when the run started its VM: the file is missing or empty, or the command failed or printed nothing. Fix the source, such as renewing the login the command reads. The run is retried. |
| `environment.smolvm.net_backend: ... use virtio-net` | `tsi`, or another backend smolvm cannot keep an allowlist on, is set. Set `virtio-net`, or remove the key. |
| `environment.smolvm.net_backend: needs allow_hosts or credentials` | A backend without an allowlist would have no network. List the hosts the VM needs in `allow_hosts`. |
| `environment.smolvm.mounts[N].guest: ... overlaps /run/sortie/control` | The mount would cover a path Sortie mounts itself. Pick another `guest`. |
| `environment.smolvm.mounts[N].host: ... does not exist` | Create the directory before Sortie loads the workflow, or put `{workspace}` in the path to have Sortie create one per issue. |
| Hooks and the agent run as root although the image sets `USER` | A pack does not carry its image's `USER`. Set `environment.smolvm.user`. |
| With `user` set, the agent cannot write `.sortie/status` | The user's uid is not the uid Sortie runs as on the host, which owns the shared `.sortie` directory. Give the pack's user Sortie's uid. |
| A nested guest fails to start, and the VM has no `/dev/kvm` | `nested` is not set, or the host's KVM module does not allow nested guests. |
| `guest workspace entry /workspace/.sortie already exists` | The repository commits a `.sortie` entry. Remove it from the repository; Sortie needs that path. |
| The agent's model calls fail, and opencode uses `opencode/big-pickle` | `models.opencode.ai` is missing from `allow_hosts`. |
| A hook cannot download dependencies | Its registry is missing from `allow_hosts`. Check `smolvm machine egress-events`. |
| The run fails with `branch moved upstream` | Someone pushed to the issue branch since Sortie's last push. The next run shows the agent that commit as `origin/<branch>`; nothing is overwritten. |
| The run fails with an export error naming `[remote rejected]` | The git host refused the push. The run's `error_detail` on `GET /api/v1/runs` and in the per-issue `history` holds the push's whole output, the host's reason included, such as a token without the `workflow` scope for a change under `.github/workflows/`. When the next run is refused the same way, Sortie parks the issue with the reason `repeated_failure` instead of retrying it again; fix the token, then remove the parking label. |
| The run fails with an export error naming the bundle size | The issue branch's bundle is larger than `max_bundle_mib`. Raise it, or keep large files out of the branch. |
| The agent's work does not reach the branch | The agent did not commit. Only committed work is exported. |
| The session opens with a tool-server gap and the agent has no Sortie tools | The agent did not reach the tool server: the VM's `socat` listener is not running (Sortie starts it at each VM start), the runtime accepts no HTTP tool server, or the tool server did not start. Check Sortie's log for `declared tool server gap`. |
