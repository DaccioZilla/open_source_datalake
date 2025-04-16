base:
	docker build --target base -t dacciozilla/base_openlake:1 .

custom-airflow:
	docker build --target airflow -t dacciozilla/custom_airflow:1 .

build:
	make base
	make custom-airflow
	docker-compose up

notebook:
	docker-compose up spark-notebook
