# the-new-zookeepers
Zookeeper Reimplementation

# development
## build protobuf files for development
```bash
docker compose run --rm dev make protos
```
(or `make clean protos` if you prefer)

alternatively, to auto rebuild when protobuf files change,
```bash
docker compose up dev --watch
```

## build report pdf
see [report's readme](report/README.md)
