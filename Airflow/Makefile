get-airflow:
	docker pull apache/airflow:slim-latest-python3.9

init:
	powershell -Command "mkdir -Force dags, logs, plugins, data"
	powershell -Command "$$env:AIRFLOW_UID='50000'; $$env:AIRFLOW_GID='0'; docker compose up"

up:
	docker compose up

down:
	docker-compose down --volumes --remove-orphans

clean:
	powershell -Command "Remove-Item -Recurse -Force dags/__pycache__, logs/*, plugins/*, data/* -ErrorAction SilentlyContinue"

get-credentials:
	docker exec -it airflow-airflow-1 cat /opt/airflow/simple_auth_manager_passwords.json.generated