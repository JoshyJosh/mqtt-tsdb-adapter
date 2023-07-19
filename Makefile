.PHONY: up down test

up:
	docker-compose up -d

down:
	docker-compose down

test:
	docker-compose -f docker-compose-test.yaml up
