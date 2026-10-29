# Phase 2 — Briefing

`ap.py stage briefing`. The phase the user is actually needed for. Its job is not to collect wishes: it is to **close what cannot be built as written**, and — where the mode or depth asks — to find out what the brief got wrong while changing it is still free. Every question exists to move a row in the manifest.

**Depth decides how much gets opened up; the mode decides who closes it.** A fork found at `deep` is the same fork in every mode — in full you settle it and label the decision, in interview you put it to the user.

**When the adversarial pass runs** — mode `interview` or `manual` at any depth, or depth `deep` in any mode — read `phases/2-adversarial.md` and run it **before** the first question. Otherwise skip it.

## The interview — in rounds

**Ask a round, not a question.** One message holds the whole current frontier — every question whose answer does not depend on another unanswered one — up to four at a time, numbered. Each carries a recommended answer and a reason in half a line, so the user can reply «ок» to accept every recommendation, or «2 — таблица, остальное ок». The next round is built from the answers: what they opened, what they closed.

```
Три вопроса, дальше соберу сам:

1. Где хранить заявки — в Google-таблице или в базе? Я бы взял таблицу: тебе её видно и не нужен сервер.
2. Кто отвечает клиентам ночью? Я бы принимал заявки круглосуточно и обещал ответ в рабочие часы.
3. Отменить заявку можно в любой момент или только до приезда мастера? Я бы — до приезда.

Ответь номерами или «ок», если согласен со всеми.
```

Before each round goes out: `ap.py ask briefing --note "три вопроса — где хранить заявки, ночь, отмена"`. The answers close it — `answered`, or the next round's `ask`.

- **Every question names its requirement** — internally, «this closes R07». A question that closes no row is dropped.
- **Look facts up, ask only decisions.** What stack the repo uses is a fact; which payment provider they have an account with is a decision.
- **Blocking unknowns go in the first round** — payment, hosting, which accounts exist, where the data lives, an existing system to fit into. Asked at the end, they cost half the project.
- **Decisions, never secrets.** *Which* provider, *whether* an account exists — yes; the key itself — never.
- **Never answer for the user.** «Не знаю» → the row stays live with the missing fact named in Основание, and the build puts a visible stub there.
- **Ask what the brief leaves open — however many that is, including none.** The mode moves the line; the brief decides how much falls on each side of it:

| | Which forks reach the user |
|---|---|
| **semi** | the ones where the branches lead to a visibly different product; the rest you settle and record |
| **interview · manual** | all of them, plus everything the adversarial pass opened |

- **Everything else you decide yourself** — error wording, retry policy, defaults, naming, layout — in every mode. But a decision goes back to the user, not to you, if it **costs money or ties them to a vendor**, **changes what they see or can do**, **means rebuilding rather than editing to undo**, or **encodes a rule about their business** — prices, deadlines, who may do what, what happens to someone's data. Unsure which side it falls on → ask.
- **Nothing open → say so and go:** «Вопросов нет — в задаче всё однозначно, пишу спецификацию», and `ap.py stage briefing skip --note "вопросов не потребовалось"`.

Calibration, not a target: semi usually lands at one or two rounds; interview and manual at three to six on a real project. What makes an interview bad is padding, never length. Say once, if it will be long, that it will be — and do not apologise for the count as you go.

In interview and manual the thing to guard against is **drift into process**: «какой стек берём?», «нарезать помельче?», «показать спецификацию?» are questions Autopilot answers itself.

## What to ask about

In priority order, only what is actually unresolved for this brief:

1. **Blocking externals** — payment, hosting, domain, third-party accounts, data to import.
2. **What the adversarial pass found**, where it ran — the only questions that can change *what* gets built.
3. **Implicit requirements** (`R##i`) — «заявки будут падать в таблицу — тебе нужен ещё экран, чтобы их смотреть, или таблицы хватит?»
4. **Depth only the user can settle** — «клиент отменил через час — деньги возвращаем сами или мастер решает?» At strict, only clarifying what they already said.
5. **Untestable wishes** — «красиво», «удобно»: cheapest turned checkable by asking what it should be **like** (below).
6. **Contradictions inside the brief** — quote both halves, ask which wins.
7. **Scope edges** — what is explicitly *not* needed; the answers become `dropped` rows and save whole tickets.

**What the last run left** — `carried.md` → «Не доделано» and «Известные баги», when the file exists — is **one question in the first round**, whatever else the round holds, and it keeps a round from being skipped: «С прошлой сборки остались: 1) корзина, 2) даты в двух форматах. Взять что-то в эту? Я бы взял 2 — это пять минут». A yes is the user adding a requirement: a `G##` row with their answer and the item it names, and the same words under `## Дополнения`. A no, or no answer to that item, leaves it in the progress file — nothing is dropped for being ignored. In full nothing is offered and nothing taken.

## Эталон — what it should be like

Whatever the user hands you that the result could be measured against — reference sites, a competitor, screenshots, a text whose tone they like, «как в приложении банка» — goes into `.autopilot/<dir>/reference.md`, one line each, verbatim after redaction, with where it came from. It travels to **the executors of tickets that build a surface** (`phases/5-subagents.md`), so the first version already looks like what they meant.

- **Only what the user gave.** A comparable you chose is your taste with a citation. Nothing named → no file.
- **A comparable is not a requirement** — it never becomes a row and never gates anything. «Тёмная тема обязательно» is a requirement and goes to the manifest.
- **One question, only where there is a surface**, folded into a round. In full, `reference.md` holds only what the brief itself carries.

## Recording answers

After each round, update the manifest rows at once: a decision into Основание; a cancellation → `dropped` with the user's words; something new → a `G##` row with their phrasing; «не знаю» → the missing fact into Основание. Every answer that **cancels, adds or reverses** is also appended to the brief's `## Дополнения`, dated and verbatim.

## When the задача changes later

«Убери SMS» at ticket four, «а добавь ещё экспорт» during a review — ordinary, and one procedure wherever it arrives:

1. **The brief file first** — their words under `## Дополнения`. First because it is the step that gets skipped.
2. **The manifest** — `dropped` with the quote, or a new `G##`.
3. **The plan** — a new `G##` becomes a ticket, a `deferred` row, or a line in the report. Say which and what it does to the rest: «Беру, но лендинг тогда сдвигается». A ticket means the whole path: a story in the spec, the row `in-spec`, a ticket file, `ap.py tickets` and `ap.py check-plan` — a row that skips the spec is invisible to both.

`G##` is the user's words, `A##` your idea, `D##` what the build proved — never blur them.

## Full mode — the self-briefing

No interview: `ap.py stage briefing skip --note "полный автомат — самобрифинг"`. Run the same list against yourself and write the answers into the manifest.

- **Decisions are yours** — stack, structure, provider, data model, layout. Pick what runs on the user's machine **without a third-party account and without money**, and record `ASSUMPTION — принято за пользователя: …` in Основание, plus `ap.py add debt.assumptions "…"` for the dashboard; every one is a line in the report.
- **Facts about the user are not** — prices, texts, addresses, business rules, brand colours: the missing fact named in Основание, a visibly labelled stub in the code (`[ЦЕНА — впиши]`, never `4990 ₽`), a line in the report.
- **A paid or account-bound service becomes an adapter** — one interface, a local stub behind it, the credential an empty name in `.env.example`.

## Closing

Gate **G1**: every row has a status; nothing `open` without a recorded reason. Then one line — «Понял. Пишу спецификацию» — and Phase 3. Do not summarise the interview back to the user.
