DOCKER := docker
DOCKER_COMPOSE := docker compose
DIR := pelias/projects/belgium_bepelias
PELIAS := "$(PWD)/pelias/pelias"
BESTTOOLS_VERSION := 1.5.0

REGION ?= all
ACTION ?= all

.PHONY: help build build-api build-pelias feed run run_pelias run_api \
    down down_api down_pelias cleanup cleanup-pelias cleanup-bepelias cleanup-folder

help:
	@echo "Targets:"
	@echo "  make build                             # Build all services (api, dataprep, pelias)"
	@echo "  make build-besttools                   # Build the BeST tools"
	@echo "  make build-dataprep                    # Build the dataprep service"
	@echo "  make build-api                         # Build the api service"
	@echo "  make build-pelias                      # Build the pelias service"
	@echo "  make feed                              # Prepare data files and feed them to Pelias; usage: make feed ACTION=all|prepare_csv|update|clean REGION=all|bru|wal|vlg"
	@echo "  make run                               # Start the API and Pelias services"
	@echo "  make stop                              # Stop the API and Pelias services"
	@echo "  make cleanup                           # Stop services and remove all containers, images and volumes; usage: make cleanup"

build: build-api build-pelias build-besttools build-dataprep

build-besttools:
	mkdir -p $(CURDIR)/best-tools-artifact
	wget https://github.com/Fedict/best-tools/archive/refs/tags/$(BESTTOOLS_VERSION).zip
	unzip $(BESTTOOLS_VERSION).zip && rm $(BESTTOOLS_VERSION).zip
	$(DOCKER) run -it --rm --name best-tools -v "$(CURDIR)/best-tools-$(BESTTOOLS_VERSION)/java":/usr/src/mymaven -w /usr/src/mymaven maven:3-eclipse-temurin-17-alpine mvn clean install -DskipTests
	cp best-tools-$(BESTTOOLS_VERSION)/java/converter/target/converter-$(BESTTOOLS_VERSION).jar $(CURDIR)/best-tools-artifact/
	cp best-tools-$(BESTTOOLS_VERSION)/java/unzip/target/unzip-$(BESTTOOLS_VERSION).jar $(CURDIR)/best-tools-artifact/

build-api:
	$(DOCKER_COMPOSE) build api

build-dataprep:
	$(DOCKER_COMPOSE) build dataprep

build-pelias:
	$(CURDIR)/scripts/build_pelias.sh

feed:
	$(CURDIR)/scripts/feed.sh $(ACTION) $(REGION)

run: run-pelias run-api

run-pelias:
	cd $(DIR) && \
	$(PELIAS) compose up

run-api:
	$(DOCKER_COMPOSE) up -d --no-deps --remove-orphans api

stop: stop-api stop-pelias

stop-api:
	$(DOCKER_COMPOSE) down

stop-pelias:
	cd $(DIR) && \
	$(PELIAS) compose down

cleanup: cleanup-pelias cleanup-api cleanup-folders

cleanup-api:
	$(DOCKER_COMPOSE) down --remove-orphans --rmi all

cleanup-pelias:
	cd $(DIR) && \
	$(PELIAS) compose down --rmi all

cleanup-folders:
	rm -rf $(CURDIR)/pelias
	rm -rf $(CURDIR)/data
	echo "Advice: try also to run: \n\
	 - docker system prune -a -f \n\
	 - docker volume prune -f"
