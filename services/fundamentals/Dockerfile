FROM python:3.11-slim

WORKDIR /app

COPY services/fundamentals/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY services/fundamentals/ .

CMD ["python", "main.py"]
