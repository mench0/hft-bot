# Phase 6 — Review

Two moments, both done by someone who did not write the code:

- **Point review during the build** — for tickets whose file says `Ревью: да` (the foundation and the narrow list of risks, `phases/4-plan.md`), and for a ticket whose contract reports one of its own requirements as not done. **After the commit, never before it**: the ticket lands on a green check, the next wave flies, and the review runs beside it. At T0 there is none: the whole-branch review is that ticket's review.
- **The whole-branch review** — once, after the last ticket, as the `review` stage. **Always**, at every tier and in every mode.

Every other ticket is reviewed only as part of the branch. Why this shape, with the measurements behind it: `docs/autopilot/design.md` in the skill's repository.

## Three axes, reported separately

| Axis | Question | Fails when |
|---|---|---|
| **Manifest** | does it deliver what the user asked for, in their words? | a requirement quietly shrank |
| **Spec** | does it implement what the spec decided? | the executor improvised |
| **Craft** | is the code fit to build on? | it works today and blocks tomorrow |

Merged or ranked together, one axis masks another. **Which axis a finding belongs to: could the executor have known?** It saw its ticket, the spec sections named, `interfaces.md`. Yes → Spec or Craft, fixed in this ticket. No → Manifest, and the defect is in the spec or the cut — repair those first, never re-run the executor against words it was never given.

## Point review

**One fresh subagent per review**, on the strong model — never a reviewer kept alive across tickets.

**Launch it in the background right after the commit** — `ap.py ticket NN review`, then the subagent — and go on with the build: launch the next wave, land other tickets. Waiting on a point review is never a reason to hold a wave.

It gets, by path: **`prompts/review.md`** (`skillDir`), required reading before the diff; the ticket file — its brief quotes are the Manifest axis; the spec sections the ticket names; `interfaces.md`; whatever the repo documents about how code is written. Pasted: the executor's `TESTS` and `REDPROOF` lines, and **why it is reviewed** — «фундамент», the named risk, or «оговорка о требовании»: that decides what blocks. And the command that shows the ticket's commit — `git show <commit>`. It must not repair, refactor, invoke skills or spawn agents.

What comes back is a verdict per axis and a `BLOCKING` line. Nothing blocking → `ap.py ticket NN done` again, and the ticket is finished. `BLOCKING` → `phases/5-repair.md`, verbatim: the fix is a second commit of the same zone. `FINDINGS` → `ap.py add concerns "…" "…"`, one call, each with its file and line. Neither the diff nor the reasoning reaches you.

**When a blocking fix changes an interface** that tickets now in flight call, append the change to `interfaces.md` and send each of them one line when it returns — before its commit.

**The re-review of a repair** — mode «ремонт» in `prompts/review.md`, again a fresh subagent, **at most once per ticket**. It gets `git diff <ticket commit>..<repair commit> -- <zone>` — the fix, not the whole ticket again — and the conditions the repair had to close, and answers each closed or not.

## What blocks

**Only the reviewer's `BLOCKING` line triggers a repair**, and what may appear there is short:

- **Manifest `partial` or `missing`** — a requirement the user asked for is not delivered.
- **An invented fact** — a plausible price, address or text where the user's own fact belongs.
- **Spec *extra*** — surface nobody asked for, unless the rest genuinely needs it.
- **A red check**, and **a test bound to internals** the next ticket has the right to rewrite.
- **In the foundation, a defect the following tickets will build on** — a wrong key in the schema, a shared client that fails the next ticket's call, access that ignores the boundary the spec drew.
- **For a risky ticket, a hole in that risk** — the security list in `prompts/review.md`.

Everything else waits for the whole-branch review. **You do not widen the list**: not «блокирующего нет, но раз уж нашлось», not a finding from `FINDINGS` you happen to agree with. That is the cost rule in `SKILL.md` — widening it is a line to the user with its price, never a quiet decision.

## The whole-branch review — the `review` stage

After the build: `ap.py stage review`.

**One fresh subagent** on the strong model — at T3 with a large diff, up to three in parallel, each given a group of zones. It gets `prompts/review.md` (mode «ветка»), the range `git diff <baseCommit>..HEAD` (`baseCommit` in `state.js`) to run itself, `spec.md` and `interfaces.md` by path, the repo's conventions, and the deferred findings from `state.js` → `concerns`, so it confirms or drops them rather than rediscovering them. It may open files outside the diff where an interaction needs it. The Manifest axis is not judged here — the blind acceptance in Phase 8 closes it against the brief.

It returns every finding sorted into three: **fix now** — it costs more to leave than to close, and the fix is bounded; **report** — real, not worth holding delivery for; **drop** — taste, or the code it pointed at is gone. Anything repeated across three or more tickets is **fix now**: that is a convention the project never settled. A security finding is fix now or report — never drop.

**All the fix-now findings become one ticket, `F1`**: `tickets/F1-<slug>.md` with the requirements the fixed code serves, `Зависит от: —`, the wave after the last one, the union of the files named as its zone, `Ревью: да — ремонт ветки`, `Модель: сильная`, and the findings as its acceptance criteria, verbatim. `ap.py tickets`, one executor, the check, one re-review in mode «ремонт» — what it still finds open goes to the report, not to a second round — one commit — never by you, never by the reviewer: a fix that skips the ticket path skips the rollback point and the green check. Every «report» finding → `ap.py add report "…" "…"`, in words the user understands; the final report reads them from there.

Then `phases/8-final.md`.

## Reporting

To the user, nothing per ticket. After the whole-branch review, one line: «Код-ревью: нашлось 7 замечаний, 4 поправил, 3 — в отчёте».
