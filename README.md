## AWS Hive Join Optimization
   This repository demonstrates how to optimize join operations in Apache Hive using AWS Glue and Amazon EMR. The project architecture includes:

- **ETL with AWS Glue**: Generate and partition datasets, storing them in S3.
- **External Tables in Hive**: Define tables in Hive pointing to S3 data.
- **Optimized Joins**: Implement Broadcast Joins, Bucketed Map Joins, and Sort-Merge Joins.
- **Step Functions Orchestration**: Manage the workflow with AWS Step Functions, running Glue jobs and EMR tasks sequentially.


## Setup Instructions

1. **AWS Glue Jobs**
   - Create two AWS Glue jobs: `products-job` and `ratings-job`.
   - Upload the scripts from the `glue_jobs/` directory to the respective Glue jobs.

2. **EMR Scripts**
   - Upload the scripts from the `emr_scripts/` directory to an S3 bucket.

3. **AWS Step Functions**
   - Create a state machine in AWS Step Functions using the definition in `state_machine_definition.json`.
   - Update the state machine definition with your specific job names, S3 bucket names, and EMR cluster configuration.

4. **Run the Workflow**
   - Start the state machine in AWS Step Functions to run the entire workflow.
   - The workflow will execute the Glue jobs in parallel, create the EMR cluster, and run the Hive scripts to create external tables and perform optimized joins.

## Note

Ensure that all necessary IAM roles and policies are in place to allow the services to interact with each other and access the required resources.
