# agensh-stack

Réplica do protocolo Agensh (arXiv 2609.26781) para teste local.

Componentes:
- `board/` — contexto compartilhado (log append-only, entradas tipadas
  OBSERVED / FACT / FAIL / CLAIM / PATCH_SUMMARY), FastAPI + SQLite.
- `router/` — loop de cooperação para N workers autoorganizados,
  dirigido a eventos, com detector de ociosidade. Fala com Gitea
  (workspace), Mattermost (mensagens) e board (contexto).

O stack é deployado no Coolify como um application isolado
(`build_pack: dockercompose`) no mesmo projeto do Gitea e do Mattermost.
