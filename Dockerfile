# builder
FROM avataa/python:3.11.13-slim-trixie AS builder-image

# install requirements
COPY pyproject.toml .
RUN uv sync --no-cache


# runner
FROM python:3.11.13-slim-trixie AS runner-image

# envs
ENV PYTHONUNBUFFERED=1

# install packages
RUN pip install -U --no-cache-dir setuptools

# add worker user
RUN adduser --disabled-password --gecos "" worker

# copy and activate virtual environment
COPY --from=builder-image --chown=worker:worker /home/worker/.venv /home/worker/.venv
ENV PATH="/home/worker/.venv/bin:${PATH}"

# copy project
COPY --chown=worker:worker app /home/worker/app

# login as worker user
USER worker

WORKDIR /home/worker/app

EXPOSE 8000

# run app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]