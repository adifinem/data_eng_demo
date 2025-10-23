FROM python:3.12-slim
WORKDIR /dbt
ENV PYTHONUNBUFFERED=1

# minimal system deps for dbt + duckdb
RUN apt-get update && \
	apt-get install -y --no-install-recommends git build-essential && \
	rm -rf /var/lib/apt/lists/*

# install dbt and duckdb adapter
COPY requirements.txt .
RUN pip install --upgrade --no-cache-dir -r requirements.txt

COPY . /dbt
ENTRYPOINT ["tail","-f","/dev/null"]

