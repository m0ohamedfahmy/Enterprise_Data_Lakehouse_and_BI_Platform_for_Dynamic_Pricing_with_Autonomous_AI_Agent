import pyspark
import glob
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, to_timestamp
import os

all_jars = glob.glob("/home/docker/.ivy2/**/*.jar", recursive=True)
jars_string = ",".join(all_jars)

def spark_config():
    NESSIE_URI = os.getenv("NESSIE_URI")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

    missing = [k for k, v in {
        "NESSIE_URI": NESSIE_URI,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing required env var(s): {', '.join(missing)}. "
            f"If running via Airflow DockerOperator, pass them explicitly via "
            f"the operator's `environment=` param — they are NOT inherited "
            f"from the worker host."
        )

    conf = (
        pyspark.SparkConf()
        .setAppName('app_name')
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,software.amazon.awssdk:bundle:2.17.178')
        .set('spark.jars.ivy', '/home/docker/.ivy2')
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', 's3a://warehouse')
        .set('spark.sql.catalog.nessie.s3.endpoint', 'http://minio:9000')
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set("spark.sql.catalog.nessie.cache-enabled", "false")
        .set("spark.driver.extraJavaOptions", "-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog")

        # S3FileIO reads THESE catalog-scoped keys, not fs.s3a.*
        .set('spark.sql.catalog.nessie.s3.access-key-id', MINIO_ACCESS_KEY)
        .set('spark.sql.catalog.nessie.s3.secret-access-key', MINIO_SECRET_KEY)
        .set('spark.sql.catalog.nessie.s3.path-style-access', 'true')  # needed for MinIO
        .set('spark.sql.catalog.nessie.s3.region', 'us-east-1')
    )
    return conf
        
def build_app(conf): 
    """
    Initializes and configures a PySpark Session with optimized log management.

    This function sets up a SparkSession using the provided configurations, enforces 
    case-insensitive column resolution for SQL queries, and suppresses non-critical 
    logs (INFO/WARN) to clean up terminal outputs.

    Parameters:
    -----------
    conf : SparkConf
        A PySpark SparkConf object containing custom configuration settings for the session.

    Returns:
    --------
    SparkSession
        An active, configured PySpark SparkSession instance.

    Raises:
    -------
    RuntimeError
        If the SparkSession fails to initialize, wrapping the underlying exception.
    """
    try:
        spark = SparkSession.builder.config(conf=conf).config("spark.sql.caseSensitive", "false").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        try:
            log4j = spark._jvm.org.apache.log4j
            log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
        except Exception:
            pass
        
        print("Spark Session Is Running....")
        return spark
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Spark Session: {e}") from e
        
def remove_duplicate(df,key1,key2,key3,col_order,date_format=None):
    """
    Deduplicates a DataFrame by keeping only the most recent record per composite key.

    Uses a PySpark Window function (row_number) partitioned by a 3-column composite key 
    and ordered descendingly by a timestamp/date column to isolate the latest row.

    Parameters:
    -----------
    df : DataFrame
        The input PySpark DataFrame containing potential duplicate records.
    key1 : str
        The first column name of the composite primary/partition key.
    key2 : str
        The second column name of the composite primary/partition key.
    key3 : str
        The third column name of the composite primary/partition key.
    col_order : str
        The column name used to determine record recency (e.g., date or timestamp).
    date_format : str, optional
        Custom date format string for parsing `col_order` via `to_timestamp` (default is None).

    Returns:
    --------
    DataFrame
        A deduplicated PySpark DataFrame containing only the latest row for each key combination.
    """
    
    if date_format:
        order_col = to_timestamp(col(col_order), date_format)
    else:
        order_col = to_timestamp(col(col_order))
        
    windowSpec = Window.partitionBy(key1, key2,key3).orderBy(col(col_order).desc())

    latest_df = df.withColumn("rn", row_number().over(windowSpec)) \
              .filter(col("rn") == 1) \
              .drop("rn")
    return latest_df

def read_data(app):
    """
    Reads raw CSV datasets and applies domain-specific deduplication.

    Loads company system transactions and scraped competitor product data from CSV files, 
    then purges duplicates using `remove_duplicate` based on their respective composite keys 
    and date formats.

    Parameters:
    -----------
    app : SparkSession
        An active PySpark SparkSession instance.

    Returns:
    --------
    dict of {str: DataFrame}
        A dictionary containing cleaned and deduplicated PySpark DataFrames:
        - 'company_system': Deduplicated internal sales/store transactions.
        - 'competitor': Deduplicated scraped market/competitor data.
    """
    # read dataset
    csv_df1 = app.read.format("csv").option("header", "true").load("/app/datasets/raw_company_system.csv")
    csv_df2 = app.read.format("csv").option("header", "true").load("/app/datasets/raw_competitor_scraper.csv")
    df1_new= remove_duplicate(df=csv_df1,key1= "Store_Name",key2= "Transaction_ID",key3= "Store_ID",col_order= "Date", date_format="M/d/yyyy")
    df2_new= remove_duplicate(df=csv_df2,key1= "Scraped_At",key2= "Competitor_Name",key3= "Product_ID",col_order= "Scraped_At" , date_format="M/d/yyyy h:mm:ss a")
    return {"company_system":df1_new ,"competitor":df2_new}

def upsert_to_iceberg_table(df, target_table, key1 , key2,key3, temp_view_name, branch_name ,app):
    
    
    """Executes a smart upsert operation into an Apache Iceberg table.

    This function handles three core operational paths:
    1. Cold Start: Creates the target table and loads initial batch if non-existent.
    2. Early Exit: Performs a Left-Anti Join to bypass I/O operations if incoming batch is 100% duplicate.
    3. Conditional MERGE: Executes a NULL-safe MERGE INTO query for updates and inserts on changed rows.

    Args:
        df (DataFrame): Incoming Spark DataFrame to be ingested.
        target_table (str): Name of the destination Iceberg table in the Nessie catalog.
        key1 (str): First composite primary key column name for joining.
        key2 (str): Second composite primary key column name for joining.
        temp_view_name (str): Temporary view name used for staging SQL execution.
        branch_name (str): Active Nessie isolation branch context.

    Raises:
        Exception: Propagates underlying Spark catalog or write execution errors.
    """
    
    # Step 1: Register Source DataFrame as a Temporary View
    # Registers the input DataFrame to enable Spark SQL queries.
    #df_no_dup = df.dropDuplicates([key1,key2])
    df.createOrReplaceTempView(temp_view_name)

   
    # Step 2: Catalog Check for Table Existence (Cold Start Handling)
    # Checks whether the target table already exists in the catalog.
    target_table_with_branch = f"nessie.bronze.`{target_table.split('.')[-1]}@{branch_name}`"
    try:
        app.sql(f"DESCRIBE {target_table_with_branch}")
        table_exists = True
    except Exception:
        table_exists = False

    
    # Path A: Table Creation & Initial Batch Ingestion
    # If the table does not exist, create it and append initial data with schema evolution enabled.

    if not table_exists:
        print(f"--- Table '{target_table}' does not exist. Creating table for the first time...")
        (
            df.write.format("iceberg")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(target_table)
        )
        print(f"--- Table '{target_table}' created and initial data loaded successfully.")
        return

    
    
    
    # Path C: Conditional MERGE Execution
    # Dynamically constructs change conditions for non-key columns and performs MERGE INTO.
    
    print(f"--- New records detected! Syncing changes to '{target_table}' (Updating existing & Adding new)...")
    non_key_cols = [col for col in df.columns if col not in [key1, key2,key3]]
    change_condition = " OR ".join([f"t.{col} <=> s.{col} = FALSE" for col in non_key_cols])
    #target_table_with_branch = f"`{target_table}@{branch_name}`"

    merge_query = f"""
        MERGE INTO {target_table} t
        USING {temp_view_name} s
        ON t.{key1} = s.{key1} AND t.{key2} = s.{key2} AND t.{key3} = s.{key3}
        WHEN MATCHED AND ({change_condition if non_key_cols else '1=1'}) THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    merge_result = app.sql(merge_query).collect()
    if merge_result:
        metrics = merge_result[0].asDict()
        
        affected_count = metrics.get("num_affected_rows", 0)
        updated_count = metrics.get("num_updated_rows", 0)
        inserted_count = metrics.get("num_inserted_rows", 0)
        deleted_count = metrics.get("num_deleted_rows", 0)
    else:
        inserted_count = updated_count = affected_count= deleted_count = 0


    print(f" Data sync completed successfully for '{target_table}':")
    print(f"   ├─  Affected (Changed) : {affected_count:,}")
    print(f"   ├─  Inserted (New Rows) : {inserted_count:,}")
    print(f"   ├─  Deleted  (Changed) : {deleted_count:,}")
    print(f"   └─  Updated  (Changed)  : {updated_count:,}")
    
    
    

def ingest_and_validate(df1 ,df2,branch_name,target_table1,target_table2 ,app):
    """Ingests source DataFrames into isolated Iceberg tables on a Nessie branch,

    validates data integrity, and cleans up the branch if any error occurs.

    Args:
        df1 (DataFrame): Spark DataFrame containing company source dataapp
        df2 (DataFrame): Spark DataFrame containing competitor source data.
        branch_name (str): The isolated Nessie branch name for staging changes.

    Raises:
        ValueError: If source datasets are empty or data integrity checks fail.
        Exception: Captures and propagates underlying write/Spark failures.
    """
    
    # Step 1: Branch Provisioning & Context Switching
    # Create an isolated staging branch from 'main' to prevent dirty reads
    
    print("============ Ingest AND Validate Execution Started ============")
    print(f"1- Creating & Switching to isolation branch: {branch_name}")
    try:
        try: 
            app.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")
            app.sql(f"DROP BRANCH {branch_name} IN nessie")
        except Exception: 
            pass
        app.sql(f"CREATE BRANCH {branch_name} IN nessie FROM main")
        app.sql(f"USE REFERENCE {branch_name} IN nessie")
        print(f"   └─ Creating & Switching Is: DONE!")
    except Exception as e:
        app.sql("USE REFERENCE main IN nessie")
        raise RuntimeError(
            f"[Stage 1 Failed] Unable to create isolation branch '{branch_name}': {e}") from e
    
    # Step 2: Empty Source Guardrail Check
    # Validate that incoming DataFrames contain data before processing.
    print("-------------------------------------------------------------------------------------")
    print("2- Checking source datasets for data presence...")
    try:
        # dropDuplicates(["Transaction_ID","Store_ID"]) , dropDuplicates(["Competitor_Name","Product_ID"])
        count_source_data_company = df1.count()
        count_source_data_competitor = df2.count()
        if count_source_data_company == 0 or count_source_data_competitor == 0:
            raise  ValueError(
        f"Aborting Pipeline: One or both data sources are EMPTY! \n"
        f"(Company Rows: {count_source_data_company}, Competitor Rows: {count_source_data_competitor})" )
        print(f"  └─ Source data validation passed successfully:")
        print(f"     ├─ Company Data   : {count_source_data_company:,} rows found")
        print(f"     └─ Competitor Data: {count_source_data_competitor:,} rows found")    
    except Exception as e:
        try:
            app.sql("USE REFERENCE main IN nessie")
            app.sql(f"DROP BRANCH {branch_name} IN nessie")
        except Exception as cleanup_error:
            print(f"Failed to drop branch: {cleanup_error}")
        raise ValueError(f"[Stage 2 Failed] Source Data Validation Guardrail triggered: {e}") from e    

        
    # Step 3: Incremental Upsert (Merge) Execution
    # Perform upsert operations into target Iceberg tables on the branch.
    print("-------------------------------------------------------------------------------------")
    print("3- Writing data to Iceberg table and Executing Incremental Upsert to Iceberg tables...")
    try:
        upsert_to_iceberg_table(df = df1,target_table= target_table1,key1= "Store_Name",key2= "Transaction_ID",key3= "Store_ID" ,temp_view_name="company_system_view",branch_name= branch_name, app=app)
        upsert_to_iceberg_table(df = df2,target_table= target_table2,key1= "Scraped_At",key2= "Competitor_Name",key3= "Product_ID", temp_view_name="competitor_view",branch_name= branch_name, app=app)

        print("   └─ Data successfully written to isolation branch!")
    except Exception as e:
        app.sql("USE REFERENCE main IN nessie")
        app.sql(f"DROP BRANCH {branch_name} IN nessie")
        raise  RuntimeError(f"[Stage 3 Failed] Upsert operation to Iceberg tables failed: {e}") from e
    
    # Step 4: Post-Ingestion Data Integrity Check
    # Verify row counts on the target branch to ensure zero data loss.
    print("-------------------------------------------------------------------------------------")
    print("4- Performing Atomic Writes Validation Checks...")
    try:
        written_df1 = app.read.table(target_table1)
        written_df2 = app.read.table(target_table2)
        count_written_data_company = written_df1.count()
        count_written_data_competitor = written_df2.count()
        print(f"Target Company Table Total Rows on '{branch_name}': {count_written_data_company}")
        print(f"Target Competitor Table Total Rows on '{branch_name}': {count_written_data_competitor}")

    
        if count_source_data_company > count_written_data_company or count_source_data_competitor > count_written_data_competitor :
            app.sql(f"DROP BRANCH {branch_name} IN nessie")
            raise ValueError(
            f"Data Integrity Validation Failed! Branch '{branch_name}' destroyed.\n"
            f"One or both datasets experienced data loss during ingestion:\n"
            f"  • Company -> Source: {count_source_data_company} | Written: {count_written_data_company}\n"
            f"  • Competitor -> Source: {count_source_data_competitor} | Written: {count_written_data_competitor}")

        print("Validation Passed: Row counts and Schema Evolution are completely verified!")
    except Exception as e:
        app.sql("USE REFERENCE main IN nessie")
        #app.sql(f"DROP BRANCH {branch_name} IN nessie")
        raise ValueError(f"[Stage 4 Failed] Post-Ingestion Data Integrity Check failed: {e}") from e

def merge_to_main(branch_name,app):
    """
    Merges an isolated staging branch back into the 'main' branch in Project Nessie 
    and performs automated branch cleanup.

    This function executes an atomic merge operation on the Nessie catalog, incorporating 
    all staging data and schema modifications into the live production state ('main'). 
    Upon a successful merge, it switches the context back to 'main' and safely drops the 
    temporary feature/staging branch.

    Parameters:
    -----------
    branch_name : str
        The name of the temporary Nessie branch containing staging changes to be merged.
    app : SparkSession
        An active PySpark SparkSession instance with Nessie SQL capabilities.

    Returns:
    --------
    None

    Raises:
    -------
    Exception
        Re-raises any underlying SQL or catalog exception if the merge, branch switch, 
        or drop operation fails.
    """
    print("============  Merging Branch Execution Started ============")
    print(f"Merging branch '{branch_name}' into 'main'...")
    try:
        app.sql(f"MERGE BRANCH {branch_name} INTO main IN nessie")
        print("PIPELINE SUCCESSFUL: Merged to 'main'!")

    except Exception as e:
        print(f"Merge failed: {e}")
        raise e 
    finally:     
        try:
            app.sql("USE REFERENCE main IN nessie")
            app.sql(f"DROP BRANCH {branch_name} IN nessie")
            print(f"Cleaned up branch '{branch_name}'.")
            print("PIPELINE SUCCESSFUL: Data & Schema changes are now live on 'main'!")
        except Exception as cleanup_err:
            print(f"Cleanup warning: {cleanup_err}")          

def run_pipeline():
    target_table1 = "nessie.bronze.raw_company_system"
    target_table2 = "nessie.bronze.raw_competitor_scraper"
    branch_name = "ingest_dev"
    printns("============ Data Ingestion Execution Started ============")
    try:
        config_spark = spark_config()
        app = build_app(config_spark)
        df = read_data(app)
        ingest_and_validate(df1=df["company_system"] ,df2= df["competitor"],branch_name =branch_name ,target_table1=target_table1,target_table2 =target_table2 ,app=app )
        merge_to_main(branch_name,app)
        print("============ Data Ingestion Execution Finished Successfully ============") 


    except Exception as pipeline_error:
        print(f"Data Ingestion Execution Aborted Due To Error: {pipeline_error}")
        raise pipeline_error      
              
if __name__ == "__main__":
    run_pipeline()       