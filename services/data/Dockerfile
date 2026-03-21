FROM python:3.11-slim

WORKDIR /app

COPY services/data/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY shared/ ./shared/
COPY services/data/ .

CMD ["python", "main.py"]
