FROM python:3.11-slim

WORKDIR /app

COPY services/disclosure/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY services/disclosure/ .

CMD ["python", "main.py"]
