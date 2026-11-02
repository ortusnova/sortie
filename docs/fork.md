# This fork

This repository is the agentic-orchestration fork of [Sortie](https://github.com/sortie-ai/sortie). Its working branch is `plan-bench`; agentic-orchestration pins it as the `sortie/` submodule.

## Merging upstream

- Merge upstream `main` into `plan-bench` at each upstream release, or weekly when no release comes, whichever is sooner.
- Merge, never rebase: open a `merge/upstream-<date>` branch from `plan-bench`, run `git merge upstream/main`, and land it through a pull request. Rebasing would rewrite commits that agentic-orchestration's submodule pins.
- Record in the merge commit body each conflict whose resolution changed the meaning of either side: the file, what each side did, and what was kept. Record the upstream commit merged, so the next merge knows where it starts.
- Keep fork work in fork-owned packages and files, with small hooks in upstream files, so each merge stays small. Fork database migrations are numbered from 1001 and fork ADRs from 1001, so upstream's numbers never collide with them.

Last merged upstream commit: `c48b724b` (2026-10-28).

## Worker leases

Jobs for `environment.kind: remote` are matched to workers in `internal/worker`, wired into the orchestrator in `internal/orchestrator/remote.go`, configured in `internal/config/environment_remote.go` and served by `internal/server/worker.go`, following the `agent-platform/worker-api` (protocol 1) and `agent-platform/worker-dispatch` specs. The lease rule sits behind the `worker.Leaser` interface. Until upstream #1499 settles, the fork follows a fixed-duration rule (`TTLLeaser`): one worker holds a job at a time, a lease lasts its TTL from the grant or last renewal, and an expired lease ends the attempt as failed; the worker's run slot, CPUs and memory return, and its disk stays counted as a stopped machine's until the worker's next check-in. Replacing the rule means implementing `Leaser`.

## Remote runs

A remote attempt is a normal worker run (`RunRemoteAttempt` in `internal/orchestrator/remote.go`, following `agent-platform/remote-runs`): the attempt's execution environment is the job's machine, Sortie drives the agent session over the `stdio` flow through `sortie stdio-bridge`, and seeds and pushes the repository through `internal/gitsync` (`BuildSeed`, `ImportReturned`). `internal/dataplane` serves the `stdio`, `mcp`, `bundles` and `usage` flows under `/api/v1/flows/`, each request admitted by its flow's job token. The remote attempt is a tool-channel environment whose host socket is the data plane's: the session's tool server is served there as for a VM, and the `mcp` flow forwards each admitted request to it with the bearer the channel's `Authorize` hook hands it, so a tool call acts on its own job's session. No forge credential reaches a worker.

## Plan sync from the workbench

The agentic-orchestration workbench syncs the plan on its own, with no Sync step for the user. It calls `POST /api/v1/plan/sync` with `"apply": false` to preview, then applies that preview with `"apply": true` and the preview's `preview_id`. After a sync that released a story, it calls `POST /api/v1/refresh` so the released story is dispatched on the next tick instead of the next poll. Both routes take the control token. Nothing else in Sortie changes for this.

## smolvm egress with virtio-net

Answer: smolvm filters egress with `virtio-net`, and does not with TSI. Measured on smolvm 1.32.4 on 2026-11-01, on p1 (Intel, `kvm_intel nested=Y`), with throwaway Alpine 3.22 machines.

- **virtio-net filters.** A machine created with `--net-backend virtio-net --allow-host-pattern ghcr.io` reached `ghcr.io` (`HTTP/1.1 301`). `example.com` did not resolve (NXDOMAIN), `1.1.1.1` and `93.184.215.14` refused the connection, and `smolvm machine egress-events` recorded each refusal, including `github.com`, where the `ghcr.io` redirect points.
- **The default backend is the same.** Without `--net-backend`, a machine with an allowlist used the same virtio-net stack (`eth0`, resolver `100.96.0.1`) and showed the same results. So a workflow without `net_backend` keeps today's machines.
- **TSI does not filter.** smolvm refuses to create a machine with `--net-backend tsi` and any egress policy: "egress policy ... requires the virtio-net backend; TSI does not enforce it". A TSI machine has no `eth0` and uses the host's resolver.
- **A backend needs a network.** `--net-backend virtio-net` without an allowlist or a credential is refused: "--net-backend virtio-net requires --net". `--net` would open the network unfiltered.
- **Credentials need the allowlist.** A credential binding alone opens the network unfiltered, on every backend. With an allowlist that lacks the credential's host, smolvm refuses the machine. With the credential's host on the allowlist, which Sortie always adds, substitution works the same with virtio-net as without it.

So validation follows: `environment.smolvm.net_backend` accepts only `virtio-net`, refuses `tsi` and every other value, and refuses `virtio-net` without `allow_hosts` or `credentials`, each with an error on `environment.smolvm.net_backend`. Sortie never adds `--net`. The gated test `TestIntegration_NetBackendKeepsTheAllowlist` in `internal/environment/smolvm` checks the allowlist on a KVM host.

The same run measured the other host inputs. `--nested` gives the guest `/dev/kvm`, and `KVM_CREATE_VM` succeeds there; without it the guest has no `/dev/kvm`. `-v HOST:GUEST[:ro]` mounts over virtiofs: a read-only mount refuses every write, a link out of a mount does not reach the host, and smolvm accepts a mount under another one, such as under `/run/sortie/control`, so Sortie refuses that itself. A write to a mounted directory reaches the host at once, so it survives a `kill -9` of the machine, and `smolvm machine delete` leaves the directory alone.
