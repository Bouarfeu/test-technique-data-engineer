FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Extension S3 de DuckDB installée dans l'image : le dashboard n'a pas besoin d'internet au démarrage
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs')"

COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]