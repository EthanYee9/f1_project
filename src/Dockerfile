FROM python:3.13-slim
WORKDIR /app
RUN apt-get update && apt-get install -y postgresql-client  
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY csv_etl_script.py .
COPY f1_predict.py .
CMD ["python", "csv_etl_script.py"]

