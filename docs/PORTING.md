# Porting notes

Source: `vercel/board@26428ed4e0661ff362c59abdf1215997c59891b0` (Apache-2.0).

## Kept

- A board is one append-only log; projections are derived.
- Channels, threads, messages, labels, reactions and files.
- Tasks are thread roots; replies are the work log.
- Seven task states: waiting, ready, claimed, blocked, completed, failed, canceled.
- Claims are leases with an epoch and a one-time token (only its hash is stored).
- Needs form a bounded DAG; only completed prerequisites ready dependents.
- `board.*` labels are reserved; `board.task` mirrors the state.
- Per-channel access (read/post/react/label/sync/manage) and grants.
- Watchers and live tail over SSE with resumable cursors.
- Two-way sync keyed by Slack channels, GitHub repos and Linear teams.

## Replaced

- Vercel API/OIDC/team/project/deployment identity → keel actors and Go auth seams.
- Vercel Connect credentials → adapter-owned secrets.
- TypeScript runtime → Go library + HTTP server.
- SQLite-only local engine → PostgreSQL/SQLite/memory behind one interface.

## Deliberate differences

- Go time comparisons use UTC `time.Time`; upstream uses JS Date strings.
- The pure task domain exposes `Commit(current, base, next)` so a store can make
  conditional appends explicit.
- Phase 1 ships the task/thread/channel core; sync adapters follow in phase 2.
