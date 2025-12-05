# Monkey Minder
Zookeeper Reimplementation

# development
## build protobuf files for development
```bash
docker compose run --rm dev make protos
```
(or `make clean protos` if you prefer)

alternatively, if you want to auto-rebuild any time the source protobuf files are modified,
```bash
docker compose up dev --watch
```

## building server docker container
note: this is necissary before running any of the tests.
```bash
docker compose build server
```

## running tests
Make sure to build the protobuf files before testing
see [tester/README.md](tester/README.md)

## run linter
```bash
docker compose run --rm dev golangci-lint run
```

## building report pdf
see [report's readme](report/README.md)

# Division of Work
Dhananjay Surti:
- Created the Tree data store and implemented Create, Update, Delete, Clone, and getChildren methods.
- Implemented parts of the client operation (Create, Delete, Exists)
- Implemented initial doFollower functionality
- Wrote up parts of Evaluation, Testing, Implementation Details and Challenges Addressed portions of the report.
- Helped out with presentation slides.

Caleb Fringer:
- Created the initial architecture of the Raft server. 
- Designed the Raft RPC services and message protos
- Implemented logic for discovering cluster peers using a cluster config file
- Implemented connecting each server in the cluster to each other's Raft gRPC server
- Implemented heartbeat protocol using AppendEntries RPC
- Designed the concurrency controls for Raft state transitions and RPC processing using channels
- Implemented Candidate lifecycle, including fanning out vote requests, quorum check, and abdication check
- Implemented core logic of AppendEntries and RequestVote RPCs to ensure log safety
- Created sequence diagrams for service lifecycle
- Implemented leader forwarding logic for the service layer
- Implemented leader commit logic
- Collaborated on developing write-ahead log
- Tested and debugged election code
- Documented and refactored code, ensuring separation of concerns and maintainability

Adrian Guerra:
- implemented majority of high-level leader node logic
- designed & implemented log
- designed client api
- implemented client side of client api
- implemented client side of client api (again, in python, for the test runner)
- designed, and implemented majority of, serverside client session handling & client message scheduling
- designed & implemented watches API
- implemented tests, created testing helper adapted from similar code written for a previous class project of mine
- set up build scripts & docker packaging for server itself, development environment, and report building
