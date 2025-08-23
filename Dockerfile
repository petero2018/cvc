# Set default versions for Python and Poetry (arguments should come before FROM)
ARG PYTHON_VERSION
ARG PYTHON_FLAVOUR
ARG ECR_PUBLIC_REPOSITORY

# Start with a slim Python base image with the dynamic version
FROM ${ECR_PUBLIC_REPOSITORY}/python:${PYTHON_VERSION}-${PYTHON_FLAVOUR}
LABEL org.opencontainers.image.authors="oszi <osztodipeter@gmail.com>"
ARG POETRY_VERSION
# Set environment variables for Poetry and add Poetry to the path
ENV PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VERSION=${POETRY_VERSION} \
    PATH="$PATH:/root/.local/bin"

RUN apt-get update && \
    apt-get install -y curl git build-essential libssl-dev zlib1g-dev && \
    apt-get install -y libbz2-dev libreadline-dev libsqlite3-dev wget && \
    curl -sSL https://install.python-poetry.org | python3 - --version ${POETRY_VERSION} && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


# Set the working directory
WORKDIR /app

# Copy Poetry project files and install dependencies
COPY . .

# Project initialization:
RUN poetry install

# Default command to keep the container running (for testing)
CMD ["tail", "-f", "/dev/null"]