FROM python:3.12-slim
WORKDIR /srv/app
# 优先用锁定版本安装，保证演练环境与上线环境一致；旧环境没有锁文件时回退到 requirements.txt。
COPY requirements.txt requirements.lock ./
RUN if [ -f requirements.lock ]; then \
        pip install --no-cache-dir -r requirements.lock; \
    else \
        pip install --no-cache-dir -r requirements.txt; \
    fi
COPY . .
# 生命周期台账落在卷上，容器重建不丢状态。
ENV LIFECYCLE_DB_PATH=/srv/app/data/lifecycle.sqlite3 \
    LIFECYCLE_BACKUP_DIR=/srv/app/data/backups \
    LIFECYCLE_LEGACY_SEED=/srv/app/data/legacy/lifecycle-history.json
RUN mkdir -p /srv/app/data/backups
VOLUME ["/srv/app/data"]
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
