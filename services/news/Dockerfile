FROM python:3.11-slim

WORKDIR /app

COPY services/news/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY services/news/ .

CMD ["python", "main.py"]
