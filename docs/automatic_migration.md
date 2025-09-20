# Shifting left from batch to real-time

As illustrated by the Spark project, the analytical data prduct is accessible after each batch pipeline execution, and even a REST API needs to run the spark job, and then cache the result for a certain time.

The goal is to move to close to real-time processing.