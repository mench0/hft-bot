# Phase 0 — Raising the project memory

Two things happen here: **pick the file, and either write the skeleton or leave the user's file alone.** What the build appends, the full description and the ADRs are in `phases/9-memory.md`, read inside Phase 5 and in Phase 8.

## Which file

The memory file Autopilot creates is **`AGENTS.md`** — Claude Code reads it natively, and so do Codex, Cursor and most other agents. Claude Code skips it, though, whenever a `CLAUDE.md` exists in the working directory or any directory above it, and some sessions cannot read it at all; so a one-line `CLAUDE.md` importing it goes beside it. The import never loads the file twice.

Decide from what is on disk, first match wins:

| On disk | `memoryFile` | `memoryOwner` | What happens |
|---|---|---|---|
| a memory file carrying `<!-- autopilot:start -->` — an earlier run wrote it | that file | `autopilot` | updated between the markers |
| a `CLAUDE.md` or `AGENTS.md` without markers — the user's own | that file; `CLAUDE.md` if both | `user` | **not written to during the run** — Phase 8 proposes additions (`phases/9-memory.md`) |
| neither file | `AGENTS.md` | `autopilot` | the skeleton below into `AGENTS.md`, and a `CLAUDE.md` holding the single line `@AGENTS.md` |

A `CLAUDE.md` whose whole content is `@AGENTS.md` is a pointer, not a memory file — judge `AGENTS.md` instead. Never create `AGENTS.md` beside a user's `CLAUDE.md`: Claude Code would not read it, and two descriptions of one project drift.

Record both fields in `state.js`. **This is never a question for the user**, in any mode — one line in the opening block, and no waiting for a reply:

> Память проекта — `AGENTS.md` (+ `CLAUDE.md` со ссылкой).

> Память проекта — твой `CLAUDE.md`: не трогаю, в конце предложу, что дописать.

## The skeleton — only in a file that is Autopilot's

Written before anything is built, so an interrupted run still leaves something true. Everything Autopilot writes sits between the two markers:

```markdown
<!-- autopilot:start -->
# <Название проекта>

<Одна строка: что это и для кого.>

## Команды

| Команда | Что делает |
|---------|------------|
| `<установка>` | Установить зависимости |
| `<запуск>` | Запустить локально |
| `<проверка>` | Тесты, типы и линтер одной командой |

## Как здесь работает Autopilot

Сборка ведётся навыком `/autopilot`. Требования, спецификация и таски — в `.autopilot/`.
Прогресс — `.autopilot/dashboard.html`. Требование из `manifest.md` может снять
только пользователь. Прерванную сборку продолжает «продолжи автопилот».

### Правка без автопилота

Мелкий баг или доводка после сборки — обычная сессия, без прогона.

Сначала прочитай:
1. `.autopilot/PROGRESS.md` — что не доделано, известные баги, о чём договорились с пользователем.
2. Что правили после последней сборки — первая строка таблицы в «История git».
3. Только если правка их касается: почему сделано так — `docs/adr/`; что значат слова — `CONTEXT.md`;
   что именно просили — `manifest.md` в папке сборки (указана в начале `PROGRESS.md`).

После правки:
- коммит — по правилам из «История git»;
- строка в «Правки без автопилота» в `PROGRESS.md`: дата — что — почему;
- починенный пункт из «Не доделано» или «Известные баги» — удали;
- договорились с пользователем о том, что следующая сборка не должна пересматривать, —
  строка в «Договорились», его словами.

### История git

Целиком не читай — бери срез под вопрос:

| Вопрос | Команда |
|---|---|
| что правили после последней сборки | `git log --oneline "$(git log -1 --format=%h --grep='^Autopilot: сборка сдана')"..HEAD -- . ':(exclude).autopilot'` |
| всё, что делали ради требования | `git log --oneline --grep="R02"` |
| история файла | `git log --oneline -10 -- <файл>` |
| когда появился или исчез код | `git log --oneline -S "<фрагмент>"` |
| почему строка такая | `git blame -L <с>,<по> <файл>`, затем `git show <sha>` |
| что внутри коммита | `git show --stat <sha>` |

Срез длиннее ~30 коммитов — отдай его субагенту и возьми выжимку в десять строк.

Коммит: заголовок — что (`fix: даты в одном формате`), тело — почему, трейлер
`--trailer "Requirements: R02"`, если правка касается требования из `manifest.md`.
Заголовок `Autopilot: сборка сдана` пишет только автопилот — по нему находят последнюю сборку.
<!-- autopilot:end -->
```

Commands not known yet are simply absent — an invented command is worse than a missing one. Anything outside the markers belongs to the user and is never edited, moved or dropped.

On a new feature in a configured repo the file already exists: **top it up, do not rewrite it.** An Autopilot block written by an older version that lacks «Правка без автопилота» or «История git» gets them now — they are what makes a session without Autopilot find `PROGRESS.md` and leave commits the next run can trace.
