window.STATE =
{
  "version": 2,
  "slug": "strategy-discovery",
  "dir": "2026-10-29-strategy-discovery--wip",
  "title": "Поиск работающих стратегий для HFT-бота",
  "mode": "semi",
  "depth": "normal",
  "tier": null,
  "briefFile": "2026-10-29-brief.md",
  "memoryFile": "AGENTS.md",
  "memoryOwner": "autopilot",
  "skillDir": "/home/user/hft-bot/.claude/skills/autopilot",
  "baseCommit": null,
  "startedAt": "2026-10-29T15:10:23+00:00",
  "updatedAt": "2026-10-29T15:10:53+00:00",
  "finishedAt": null,
  "stages": [
    {
      "id": "preflight",
      "status": "done",
      "startedAt": "2026-10-29T15:10:23+00:00",
      "finishedAt": "2026-10-29T15:10:40+00:00"
    },
    {
      "id": "manifest",
      "status": "done",
      "startedAt": "2026-10-29T15:10:40+00:00",
      "finishedAt": "2026-10-29T15:10:44+00:00"
    },
    {
      "id": "briefing",
      "status": "active",
      "startedAt": "2026-10-29T15:10:44+00:00"
    },
    {
      "id": "spec",
      "status": "pending"
    },
    {
      "id": "plan",
      "status": "pending"
    },
    {
      "id": "build",
      "status": "pending"
    },
    {
      "id": "review",
      "status": "pending"
    },
    {
      "id": "final",
      "status": "pending"
    }
  ],
  "requirements": {
    "total": 5,
    "done": 0,
    "inTicket": 0,
    "inSpec": 0,
    "placeholder": 0,
    "deferred": 0,
    "dropped": 0
  },
  "tickets": [],
  "tests": null,
  "debt": {
    "placeholders": [],
    "assumptions": [],
    "emptyEnv": []
  },
  "additions": [],
  "coverage": null,
  "concerns": [],
  "blind": null,
  "beats": [
    "2026-10-29T15:10:23+00:00",
    "2026-10-29T15:10:40+00:00",
    "2026-10-29T15:10:44+00:00",
    "2026-10-29T15:10:53+00:00"
  ],
  "rev": 6,
  "waiting": {
    "kind": "briefing",
    "since": "2026-10-29T15:10:53+00:00",
    "note": "4 вопроса — как искать, критерий «работает», какие стратегии, где смотреть результаты"
  },
  "waits": [],
  "log": [
    {
      "t": "2026-10-29T15:10:23+00:00",
      "k": "init"
    },
    {
      "t": "2026-10-29T15:10:40+00:00",
      "k": "stage",
      "id": "manifest",
      "act": "start"
    },
    {
      "t": "2026-10-29T15:10:44+00:00",
      "k": "stage",
      "id": "briefing",
      "act": "start"
    },
    {
      "t": "2026-10-29T15:10:53+00:00",
      "k": "ask",
      "kind": "briefing",
      "note": "4 вопроса — как искать, критерий «работает», какие стратегии, где смотреть результаты"
    }
  ],
  "reqList": [
    {
      "id": "R01",
      "text": "«находить … стратегии для hft бота»",
      "status": "open"
    },
    {
      "id": "R02",
      "text": "«работающие»",
      "status": "open"
    },
    {
      "id": "R03",
      "text": "«на криптовалютных биржах»",
      "status": "open"
    },
    {
      "id": "R04i",
      "text": "(подразумевается) найденную стратегию можно запустить в боте",
      "status": "open"
    },
    {
      "id": "R05i",
      "text": "(подразумевается) результаты поиска где-то видны пользователю",
      "status": "open"
    }
  ],
  "gates": {},
  "result": null
}
