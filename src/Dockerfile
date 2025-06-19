FROM python:3.13-slim
RUN apt-get update && apt-get install -y postgres-client  
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY etl_script.py .
CMD ["python", "src/etl_script.py"]
