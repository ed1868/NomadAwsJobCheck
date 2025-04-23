# 🔍 AWS Lambda: Glue & EMR Monitoring

This AWS Lambda function monitors AWS Glue job runs and Amazon EMR cluster activity from the past 24 hours. It gathers detailed metadata about Glue runs and active EMR clusters, returning a structured JSON response with the aggregated data.

---

## 📦 Features

- Retrieves all AWS Glue jobs and filters runs from the last 24 hours.
- Enriches each Glue run with metadata (status, execution time, error messages, etc.).
- Lists all active Amazon EMR clusters (in `STARTING`, `BOOTSTRAPPING`, `RUNNING`, or `WAITING` states).
- Enriches EMR cluster data with configuration, application info, and tags.
- Supports filtering for **named clusters** to return specific ones in the final response.

---

## 📁 Structure of Response

```json
{
  "glueJobRunData": [ ... ],    // List of enriched Glue job runs
  "emrClusterData": [ ... ],    // List of enriched EMR cluster metadata
  "your-own-cluster-name": { ... } // Specific named EMR cluster info
}
