FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# uvicorn 单 worker：题目要求的“两个 API 进程”由 compose 复制两份服务实现，
# 每个容器一个进程，共享同一个 SQLite 文件，便于演示跨进程协调。
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
