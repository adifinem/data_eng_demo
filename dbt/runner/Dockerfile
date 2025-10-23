FROM python:3.12-slim
WORKDIR /runner

COPY requirements.txt .
RUN pip install --upgrade --no-cache-dir -r requirements.txt

COPY server.py /runner/server.py
EXPOSE 9001
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "9001"]
