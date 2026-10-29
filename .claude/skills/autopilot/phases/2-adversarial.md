# The adversarial pass

Read only when it runs: mode `interview` or `manual` at any depth, or depth `deep` in any mode. Run it against the brief (`<дата>-brief.md`) and `manifest.md` **before the first question**.

Everything else in the briefing asks what the brief left undefined. This asks **where the idea itself comes apart** — a brief can be complete and consistent and still describe a thing that will not work, and every later check measures the build against what was asked for, so nothing downstream will notice.

Seven questions, put to the задача, not to the user — the answers are yours to find:

| | The question |
|---|---|
| **1. Провал** | Проект сдан, работает, и им никто не пользуется. Что произошло? |
| **2. Столкновение** | Какие два требования — разные строки манифеста — не могут быть верны одновременно? |
| **3. Непроверенное** | Что пользователь считает решённым, а оно не решено? «Люди этим будут пользоваться», «данные откуда-то возьмутся» |
| **4. Цена** | Какое требование съест половину сборки ради малой доли ценности — и знает ли он об этом? |
| **5. Условие** | Без чего результат бессмысленен, хотя в брифе этого нет? Кто наполнит, кто будет обслуживать, откуда первые данные |
| **6. Вторая неделя** | Что будет, когда это перестанет быть новым: рост, дубликаты, модерация, поддержка, чужие руки |
| **7. Второй актор** | Кто ещё будет этим пользоваться, кроме описанного? |

Five to seven findings is the normal yield; fewer than three on a real project means the pass was run for form's sake.

## Where a finding goes — by mode, not by how alarming it looks

| Kind of finding | full | semi | interview · manual |
|---|---|---|---|
| only the user can settle it | `ASSUMPTION` in the manifest and the report | asked **only if** the branches give a visibly different product | **asked**, in the second round at the latest |
| craft — you can settle it | decided, into the spec | same | same |
| a whole capability | `A##` under its parent, or «Вне рамок» | same | same, or offered as a question |
| outside the задача | one line in «Вне рамок» | same | same |

**The pass removes nothing.** It produces questions, `A##` stories, «Вне рамок» lines and `ASSUMPTION` rows — never a `dropped` row, never a quietly narrowed requirement. A requirement it thinks is a bad idea earns at most one question naming the cost.

**It grills the задача, not the person.** «Заявки будут приходить — а кто их читает в субботу?» is the pass working. «Ты уверен, что это вообще кому-то нужно?» is an opinion, and nothing can be built from it.
