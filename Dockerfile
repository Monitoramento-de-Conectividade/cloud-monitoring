FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HISTORY_MODE=fresh \
    DASHBOARD_HOST=0.0.0.0 \
    DASHBOARD_PORT=8008

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/logs_mqtt /app/dashboards \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8008

CMD ["python", "cloudv2-ping-monitoring.py"]
