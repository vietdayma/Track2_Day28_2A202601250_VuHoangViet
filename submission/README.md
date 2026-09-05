# submission/ — committed copy of the evidence bundle

This folder exists only because grading here is "repo link only" (no separate
LMS/file upload). Everything under `evidence/` and `submission-screenshots/`
at the repo root is intentionally `.gitignore`d — it is runtime output the
platform regenerates on every run, per the lab's own rule against committing
generated state. `submission/` is a **point-in-time copy** taken so the same
proof travels with the repo when there is no other channel to attach it to.

- `proof/` — the 14 evidence JSON files described in `ANSWERS.md` §2
  (IP01–IP10 + the real-vLLM capture + the load profile), copied from
  `evidence/` after the run recorded in `ANSWERS.md`.
- `screenshots/` — the 6 UI screenshots referenced in `ANSWERS.md`
  (Grafana, Jaeger, MLflow, Qdrant, Airflow, Prometheus).

No secrets, tokens, or long-lived credentials are in either folder — the one
ephemeral Kaggle tunnel URL that appears in `proof/ip07-vllm-identity-verified-live.json`
was already dead by the time this was committed (see `ANSWERS.md` §2, IP07)
and grants no access.

Regenerate this snapshot after re-running the stack:

```powershell
Remove-Item submission\proof\*, submission\screenshots\* -ErrorAction SilentlyContinue
Copy-Item evidence\*.json submission\proof\
Copy-Item submission-screenshots\0*.png submission\screenshots\
```
