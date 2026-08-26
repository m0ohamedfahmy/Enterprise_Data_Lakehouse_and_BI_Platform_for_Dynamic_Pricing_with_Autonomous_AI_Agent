# RCA Report — ingestion_test.run_spark_processing

- **Run ID:** manual__2026-08-23T14:27:22.563803+00:00
- **Stage:** ingestion
- **Failure origin:** script_level
- **Category:** script_bug
- **Confidence:** 0.99
- **Diagnosis source:** groq:code
- **Generated:** 20260824T094552Z

## Root Cause
A typo in the ingestion script calls an undefined function `printns` instead of the built‑in `print`, causing a NameError at runtime.

## Log Evidence
```
Traceback (most recent call last):\n  File "/app/00_Data_ingestion.py", line 416, in <module>\n    run_pipeline()       \n  File "/app/00_Data_ingestion.py", line 401, in run_pipeline\n    printns("============ Data Ingestion Execution Started ============")\nNameError: name 'printns' is not defined. Did you mean: 'print'?
```

## Failing Script
`/home/ninja/lakehouse-docker/lakehouse/scripts/00_Data_ingestion.py`

## Proposed Code Diff
```diff
--- a/00_Data_ingestion.py
+++ b/00_Data_ingestion_recommended_20260824T094552Z.py
@@ -398,7 +398,7 @@
     target_table1 = "nessie.bronze.raw_company_system"
     target_table2 = "nessie.bronze.raw_competitor_scraper"
     branch_name = "ingest_dev"
-    printns("============ Data Ingestion Execution Started ============")
+        print("============ Data Ingestion Execution Started ============" )
     try:
         config_spark = spark_config()
         app = build_app(config_spark)
```

## Recommended Fix File
[00_Data_ingestion_recommended_20260824T094552Z.py](../recommended_fixes/00_Data_ingestion_recommended_20260824T094552Z.py) — generated, non-destructive; **not** applied automatically (recommended_action stays `alert_only`).

## Recommended Fix
Edit /home/ninja/lakehouse-docker/lakehouse/scripts/00_Data_ingestion.py and replace the erroneous call:
```python
-    printns("============ Data Ingestion Execution Started ============")
+    print("============ Data Ingestion Execution Started ============")
```
If a custom logging helper was intended, define it (e.g., `def printns(msg): print(msg)`) before use.

## Recommended Action
`alert_only`

## Original Log Tail
```
{"content":[{"event":"::group::Log message source details"},{"event":"/usr/local/airflow/logs/dag_id=ingestion_test/run_id=manual__2026-08-23T14:27:22.563803+00:00/task_id=run_spark_processing/attempt=8.log"},{"event":"::endgroup::"},{"timestamp":"2026-08-23T15:59:01.896392Z","event":"::group::Pre Execute","level":"info","logger":"task","filename":"task_runner.py","lineno":2303},{"timestamp":"2026-08-23T15:59:02.323394Z","event":"Astro managed secrets backend is disabled","level":"warning","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"astronomer.runtime.plugin.AstronomerRuntimePlugin","filename":"plugin.py","lineno":125},{"timestamp":"2026-08-23T15:59:02.339863Z","event":"DAG bundles loaded: dags-folder","level":"info","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"airflow.dag_processing.bundles.manager.DagBundlesManager","filename":"manager.py","lineno":271},{"timestamp":"2026-08-23T15:59:02.347182Z","event":"Filling up the DagBag from /usr/local/airflow/dags/pipeline.py","level":"info","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"airflow.dag_processing.dagbag.BundleDagBag","filename":"dagbag.py","lineno":463},{"timestamp":"2026-08-23T15:59:02.747459Z","event":"The `airflow.models.param.Param` attribute is deprecated. Please use `'airflow.sdk.Param'`.","level":"warning","category":"UserWarning","filename":"/usr/local/airflow/dags/pipeline.py","lineno":3,"task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"py.warnings"},{"timestamp":"2026-08-23T15:59:02.748104Z","event":"airflow.exceptions.AirflowSkipException is deprecated and will be removed in a future version. Use airflow.sdk.exceptions.AirflowSkipException instead.","level":"warning","category":"UserWarning","filename":"/usr/local/lib/python3.14/site-packages/airflow/providers/docker/exceptions.py","lineno":21,"task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"py.warnings"},{"timestamp":"2026-08-23T15:59:02.748788Z","event":"The `airflow.hooks.base.BaseHook` attribute is deprecated. Please use `'airflow.sdk.bases.hook.BaseHook'`.","level":"warning","category":"UserWarning","filename":"/usr/local/lib/python3.14/site-packages/airflow/providers/docker/hooks/docker.py","lineno":31,"task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"py.warnings"},{"timestamp":"2026-08-23T15:59:02.753481Z","event":"Worker startup parse complete","level":"info","bundle_name":"dags-folder","bundle_version":null,"dag_file":"pipeline.py","dag_id":"ingestion_test","bundle_prepare_ms":13,"dag_file_parse_ms":407,"task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","logger":"task","filename":"task_runner.py","lineno":1062},{"timestamp":"2026-08-23T15:59:03.786958Z","event":"::endgroup::","level":"info","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"task","filename":"task_runner.py","lineno":2107},{"timestamp":"2026-08-23T15:59:04.905294Z","event":"Starting docker container from image my-spark-ingestion-job:v1","level":"info","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"airflow.task.operators.airflow.providers.docker.operators.docker.DockerOperator","filename":"docker.py","lineno":359},{"timestamp":"2026-08-23T15:59:11.059608Z","event":"Traceback (most recent call last):\n  File \"/app/00_Data_ingestion.py\", line 416, in <module>\n    run_pipeline()       \n  File \"/app/00_Data_ingestion.py\", line 401, in run_pipeline\n    printns(\"============ Data Ingestion Execution Started ============\")\nNameError: name 'printns' is not defined. Did you mean: 'print'?","level":"info","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"airflow.task.operators.airflow.providers.docker.operators.docker.DockerOperator","filename":"docker.py","lineno":429},{"timestamp":"2026-08-23T15:59:14.190914Z","event":"Task failed with exception","level":"error","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"task","filename":"task_runner.py","lineno":1648,"error_detail":[{"exc_type":"DockerContainerFailedException","exc_value":"Docker container failed: {'StatusCode': 1}","exc_notes":[],"syntax_error":null,"is_cause":false,"frames":[{"filename":"/usr/local/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py","lineno":1576,"name":"run"},{"filename":"/usr/local/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py","lineno":189,"name":"wrapper"},{"filename":"/usr/local/lib/python3.14/site-packages/airflow/sdk/execution_time/task_runner.py","lineno":2125,"name":"_execute_task"},{"filename":"/usr/local/lib/python3.14/site-packages/airflow/sdk/bases/operator.py","lineno":445,"name":"wrapper"},{"filename":"/usr/local/lib/python3.14/site-packages/airflow/providers/docker/operators/docker.py","lineno":502,"name":"execute"},{"filename":"/usr/local/lib/python3.14/site-packages/airflow/providers/docker/operators/docker.py","lineno":376,"name":"_run_image"},{"filename":"/usr/local/lib/python3.14/site-packages/airflow/providers/docker/operators/docker.py","lineno":437,"name":"_run_image_with_mounts"}],"is_group":false,"exceptions":[]}]},{"timestamp":"2026-08-23T15:59:14.194235Z","event":"::group::Post Execute","level":"info","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"task","filename":"task_runner.py","lineno":1649},{"timestamp":"2026-08-23T15:59:14.224920Z","event":"::endgroup::","level":"info","task_id":"run_spark_processing","map_index":-1,"run_id":"manual__2026-08-23T14:27:22.563803+00:00","try_number":8,"ti_id":"01a02f58-b243-7f87-815c-83bf864927dd","dag_id":"ingestion_test","logger":"task","filename":"task_runner.py","lineno":2270}],"continuation_token":null}
```
