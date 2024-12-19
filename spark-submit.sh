#!/bin/bash
spark-submit \\
    --master k8s://https://<kubernetes-api-url> \\
    --deploy-mode cluster \\
    --name pyspark-job \\
    --conf spark.executor.instances=2 \\
    --conf spark.kubernetes.container.image=<docker-image-url> \\
    --conf spark.kubernetes.namespace=default \\
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \\
    local:///app/main.py
