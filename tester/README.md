# NOTE: YOU MAY NEED TO ENABLE 'host networking' IN DOCKER'S SETTINGS FOR THIS TO WORK!

# prerequisites/setup
- install [uv](https://docs.astral.sh/uv/).
- build the server docker container (see the top-level README for instructions)
- create a docker network for the nodes to be run in
  ```bash
  docker network create monkey-minder-test-network
  ```
- each server container will be bound to a non-dynamically-selected host port,
  starting at `13800` for the first node and going up by one for each subsequent node started.
  (so less than +10 total).
  if this does not work for your particular setup, edit `/tester/src/tests.py` to add e.g. `base_host_port=1234` to the
  configuration values set in `asyncSetUp`.

# run tests
to run all of the tests, first make sure that you are in the `tester` directory and then simply run
```bash
uv run mmtester
```

# development setup
while developing the testing code, you can run `uv sync` to manually create/update the python venv with our dependencies for IDE/LSP purposes.
```bash
uv sync
```

# about
this is based on [https://github.com/adrianmgg/asg3tester](https://github.com/adrianmgg/asg3tester), which one of us developed
for a different project a few years ago.
