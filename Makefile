.PHONY: pg-up
pg-up:
	docker-compose up -d postgres
	docker exec -it distlock-postgres bash -c 'while ! pg_isready; do echo "waiting for postgres"; sleep 2; done'
