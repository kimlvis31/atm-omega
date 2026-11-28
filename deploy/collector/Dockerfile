FROM python:3.13.16-slim@sha256:cf50a4f6ac4f026458b08d5222023d700ac806410dff5e9ca8728fa44f2b850f
WORKDIR /opt/omega
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir ".[postgres,streaming,vision]"
ENTRYPOINT ["python", "-I", "-B", "-m", "omega.runtime.collect_cli"]
