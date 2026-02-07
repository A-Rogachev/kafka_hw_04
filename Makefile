.DEFAULT_GOAL := start

.PHONY: help start check logs reading stop

help:   ## - Выводит список команд
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

start:  ## - Запускает все контейнеры
	sudo docker compose up -d --build --scale data-reader=0

check:  ## - Псевдоним для команды просмотра запущенных контейнеров в docker compose
	sudo docker compose ps -a

reading:  ## - Запускает контейнер data-reader
	sudo docker compose up --build data-reader

stop:  ## - Удаляет все контейнеры
	sudo docker compose down -v --remove-orphans

logs:  ## - Псевдоним для команды просмотра логов всех контейнеров в docker compose
	sudo docker compose logs -f data-writer
