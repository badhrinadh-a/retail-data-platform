.PHONY: help up down restart logs build format lint test clean

help:
	@echo "Available commands:"
	@echo "  up       : Start all services in the background"
	@echo "  down     : Stop and remove all services"
	@echo "  restart  : Restart all services"
	@echo "  logs     : Follow logs for all services"
	@echo "  build    : Build custom Docker images"
	@echo "  format   : Run black and isort to format code"
	@echo "  lint     : Run flake8 to lint code"
	@echo "  test     : Run pytest"
	@echo "  clean    : Remove python cache files"

up:
	docker-compose up -d

down:
	docker-compose down -v

restart:
	docker-compose down && docker-compose up -d

logs:
	docker-compose logs -f

build:
	docker-compose build

format:
	black src/ tests/
	isort src/ tests/

lint:
	flake8 src/ tests/

test:
	pytest tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
