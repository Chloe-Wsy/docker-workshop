FROM python:3.13.11-slim

#Makes uv available inside your image without installing it
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

#Sets /app as the working directory in the container.
WORKDIR /code

#Adds the uv-created virtual environment to PATH, so python and installed packages resolve automatically
ENV PATH="/code/.venv/bin:$PATH"

#enables Docker layer caching
COPY "pyproject.toml" "uv.lock" ".python-version" ./
RUN uv sync --locked

#Copies pipeline.py from your local directory into /app/pipeline.py in the container.
COPY ingest_data.py ingest_data.py


#When the container starts, it runs: python pipeline.py
ENTRYPOINT ["python", "ingest_data.py"]