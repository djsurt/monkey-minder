# building report
## build once
```bash
docker compose up dev-report --detach
docker compose run dev-report make report.pdf
```

## watch & auto-rebuild
```bash
docker compose up dev-report --watch
```

