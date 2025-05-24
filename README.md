# sight-stack
Commands I used in wsl -d Ubuntu (root@Houda:/mnt/c/Users/htagi#): 
cd kafka-spark-hbase ################## docker compose up -d ################### docker ps --format "table {{.Names}}\t{{.Status}}" ################### pip install
kafka-python
requests
happybase
python-dateutil
pandas
pyspark ################## python3 -m venv stackoverflow-env #################### cd ~ #################### source venv/bin/activate ################# docker run --rm -it
--network kafka-spark-hbase_default
bitnami/kafka:latest
kafka-topics.sh
--create
--topic stackoverflow-questions
--bootstrap-server kafka:9092
--partitions 1
--replication-factor 1 #################### create the second topic: kafka-topics.sh
--bootstrap-server localhost:29092
--create
--replication-factor 1
--partitions 1
--topic stackoverflow-trends ######################### python3 stack_api_to_kafka.py ##################### spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_stream_to_hbase.py




Now I want you to help me do the part of spring and the front react JS hereare the steps and connect the hbase with spring :

You are responsible for building and orchestrating the full StackSight pipeline:

1. *Kafka Ingestion*  
   - Consume two topics from Kafka on bootstrap localhost:29092:  
     • stackoverflow-questions (JSON payload per message)  
     • stackoverflow-trends (JSON payload per message)  
   - Example stackoverflow-questions message schema:
     {
       "question_id": Long,
       "title": String,
       "body": String,            // may contain HTML: <p>, <code>, <pre>, <a>, <img>, <br>, etc.
       "creation_date": Long,     // epoch seconds
       "score": Integer,
       "tags": [String],          // always present via filter=!9_bDE(fI5
       "owner_reputation": Integer,
       "answers": [
         {
           "answer_id": Long,
           "body": String,         // may contain HTML
           "score": Integer,
           "is_accepted": Boolean,
           "owner_reputation": Integer
         },
         …
       ]
     }
   - Example stackoverflow-trends message schema:
     { "tag": String, "count": Integer }

2. *HTML Stripping & Tag Handling*  
   - In Spark, strip *all* HTML tags from body fields (both questions and answers), preserving only inner text.  
   - Do *not* attempt to re-render HTML in Spark; store raw text. Front-end will render code blocks and links safely in Thymeleaf.

3. *Spark Structured Streaming Preprocessing*  
   - Use Spark 3.2 Structured Streaming (spark.readStream).  
   - Define a *1-hour tumbling window* on creation_date with a *10-minute watermark* for late data. Also maintain daily (calendar-day) and monthly (calendar-month) aggregates via separate batch queries.  
   - For each (tag, window) compute:
     1. Total questions  
     2. % unanswered (answers array empty)  
     3. % accepted (answers array contains at least one is_accepted=true)  
     4. Average question score  
     5. Average answer score  
   - Output these aggregates into a DataFrame stream for trends, and write raw question+answer rows for search.

4. *HBase Storage*  
   - Table stackoverflow_qna, row key = question_id, column families:
     - question: → title, body, creation_date, score, owner_reputation  
     - answers: → for each answer index (1…n): answer{i}_id, answer{i}_body, answer{i}_score, answer{i}_is_accepted, answer{i}_owner_reputation
   - Table stackoverflow_trends, row key = <tag>#<YYYYMM>, column family trend:, qualifier count.

5. *Top-3 Answer Selection*  
   - For each question, pick answers in this order:
     1. If there’s an accepted answer, include it first.  
     2. Then the highest-scoring answers whose owner_reputation > 1000, until you have 3.  
     3. If still under 3, fill with next-highest-score answers regardless of reputation.  
   - Attach these top-3 to the question row in stackoverflow_qna.

6. *Spring Boot Backend*  
   - Expose REST endpoints secured with JWT:
     - GET /api/search?q={query}&tags={tagList}&limit=10  
       • Returns matching questions with their top-3 answers.  
     - GET /api/trends?tag={tag}&period={hour|day|month}  
       • Returns the time series aggregate for that tag and period.  
     - GET /api/suggest?prefix={input}  
       • Returns up to 5 question titles for autosuggest (called after ≥2 characters with 300 ms debounce).

7. *Pure react JS Front-End*  
   - Use Thymeleaf templates styled with Bootstrap.  
   - On the search page:
     - An input box with pure-JS debounce (300 ms) calling /api/suggest for live autosuggest.  
     - On submit, call /api/search and render questions with their top-3 answers. Preserve code blocks (e.g. <pre>, <code>) and hyperlinks safely.
   - On the trends page:
     - Dropdowns for tag selection and period (Hour/Day/Month).  
     - Fetch /api/trends and render line/bar charts (client-side library of your choice).

8. *Non-functional Requirements*  
   - Search API: 95th-percentile latency < 500 ms.  
   - Trend aggregations update every 5 minutes (stream + batch).  
   - Secure JWT authentication on all endpoints.
