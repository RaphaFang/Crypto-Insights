# Bitcoin Insights
This project is designed to practice **real-time data engineering** and **business intelligence visualization**.  

The workflow combines **Kafka** as pipeline, **Flink** as streaming data aggregation, **Clickhouse** for data storage, and **Grafana** as dashboards.

---
## Progress at branch "foundation" so far

1. Connect real time trading data of Bitcoin from _**Binance WebSocket**_, and send it to Kafka.

2. _**Flink**_ gets the data, and turn it into a 10s volume-weighted average price -- which only having a 2~5s latency. After the aggregation, send the data back to different topic of Kafka.

3. _**Clickhouse**_ get the data from Kafka, to ensure data persistence, decouple from Kafka, and keep the system expandable for BI in the future.

4. _**Grafana**_ is used as the dashboard, connects to data from ClickHouse, and displays the 10-second VWAP.