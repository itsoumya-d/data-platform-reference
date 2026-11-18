FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY src/ src/
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD curl -f http://localhost:8000/health || exit 1
CMD ["uvicorn", "dataplat.api:app", "--host", "0.0.0.0", "--port", "8000"]
