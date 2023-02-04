run:
	docker compose up
del:
	docker compose rm -s -f
	docker rmi clickhouse_compose-client