FROM python:3.12-slim as builder

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY ./requirements.txt .

RUN pip wheel --no-cache-dir --no-deps --wheel-dir wheels -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends mariadb-client=1:10.11.4-1~deb12u1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/wheels /wheels
# hadolint ignore=DL3013
RUN pip install --no-cache-dir /wheels/*

COPY . .

ENTRYPOINT ["python", "main.py"]
