run:
	docker compose up
del:
	docker compose rm -s -f
	docker rmi clear_1-client