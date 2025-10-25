# building report
## build once
```bash
docker compose run --rm dev-report make report.pdf
```
or
```bash
docker compose up dev-report --detach
docker compose exec dev-report make report.pdf
```

## watch & auto-rebuild
```bash
docker compose up dev-report --watch
```

