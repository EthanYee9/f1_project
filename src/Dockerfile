FROM python:3.13-slim
RUN apt-get update && apt-get install -y postgresql-client  
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY csv_etl_script.py .
CMD ["python", "src/csv_etl_script.py"]
