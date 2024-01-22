# GCP Spark JSON to Avro Example
This Repository creates a dataproc cluster, which transforms incoming JSON files to Avro via Spark.  The job stores sucessfully converted files in a processed bucket and then destroys the cluster.  The transformation is performed via a pre-provided Avro schema.  

This sample code also defines a Google App Engine application that periodically checks for new Avro files on a bucket via App Engine Cron Service, sending a message to Pub/Sub (google managed Kafka).  A GCE instance polls this topic, launching a new Dataproc cluster to perform the JSON to Avro transformation.
