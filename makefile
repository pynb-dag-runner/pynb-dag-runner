SHELL := /bin/bash

### Tasks to build Docker images

docker-build:
	docker \
	    build \
	    --file Dockerfile \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag pynb-dag-runner .

JUPYTER_TOKEN:
	# Create random JUPYTER_TOKEN for Jupyter running in Docker
	openssl rand -base64 42 > JUPYTER_TOKEN

docker-build-dev: JUPYTER_TOKEN
	# Build docker image for (local) development
	make docker-build

	docker \
	    build \
	    --file Dockerfile.dev \
	    --build-arg JUPYTER_TOKEN=$$(cat JUPYTER_TOKEN) \
	    --tag pynb-dag-runner-dev .

### Manually start/stop the dev-docker container (can be used without VS Code)

dev-up:
	docker-compose up --remove-orphans --abort-on-container-exit dev-environment

dev-down:
	docker-compose down --remove-orphans

### Define tasks run inside Docker

docker-run:
	# Run bash command(s) in (non-dev) Docker container.

	docker run --rm \
	    --network none \
	    --volume $$(pwd)/workspace:/home/host_user/workspace \
		--env RUN_ENVIRONMENT=$(RUN_ENVIRONMENT) \
	    --workdir /home/host_user/workspace/ \
	    pynb-dag-runner \
	    "$(COMMAND)"

clean:
	make COMMAND="(cd pynb_dag_runner; make clean)" docker-run

test-library:
	# Run all tests for library and ensure we can build the library wheel file
	#
	# Eg.
	#   make test-library
	#   make RUN_ENVIRONMENT=stress-tests test-library

	make \
	    RUN_ENVIRONMENT=$(RUN_ENVIRONMENT) \
	    COMMAND="(\
	        cd pynb_dag_runner; \
	        make \
	            test-pytest \
	            test-mypy \
	            test-black \
	            build \
	)" docker-run
