up:
	docker compose up -d
	docker exec -it spark pip install pandas
	bash scripts/fetch_jars.sh

down:
	docker compose down -v

logs:
	docker compose logs -f

fetch-jars:
	bash scripts/fetch_jars.sh

step1:
	docker compose exec spark spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/pipelines/01_create_and_insert.py

step2:
	docker compose exec spark spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/pipelines/02_partition_and_evolve.py

step3:
	docker compose exec spark spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/pipelines/03_merge_upsert.py

step4:
	docker compose exec spark spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/pipelines/04_time_travel.py

step5:
	docker compose exec spark spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/pipelines/05_schema_evolution.py

demo:
	bash scripts/interactive_demo.sh
