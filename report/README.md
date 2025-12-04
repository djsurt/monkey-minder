# building report
## files
- `report.pdf` is the final version of the report
- `report-draft.pdf` is the report with TODOs included, and a list of TODOs at the top of the document
- `report-nofigures.pdf` is `report.pdf` but with all figures excluded

anywhere subsequent build instructions use `report.pdf`, one of these other targets can be substituted

## build once
```bash
docker compose run --rm dev-report make report.pdf
```
or, to re-use the same container:
```bash
docker compose up dev-report --detach
docker compose exec dev-report make report.pdf
```

## watch & auto-rebuild
```bash
docker compose up dev-report --watch
```

