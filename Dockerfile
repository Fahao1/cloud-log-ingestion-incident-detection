FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /service
COPY pyproject.toml requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY app app
COPY migrations migrations
COPY scripts scripts
COPY tests tests
COPY licenses licenses
COPY LICENSE THIRD_PARTY.md ./
RUN useradd --create-home --uid 10001 service && chown -R service:service /service
USER service
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
