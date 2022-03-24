SHELL:=/bin/bash

TAG ?= latest
VERSION = $(shell git rev-parse --short=8 HEAD || echo latest)
REGISTRY = k3d-myregistry.localhost:5000

.PHONY:
mysql_init:
	docker build -t mysql:$(VERSION) -f ./mysql/Dockerfile .
	docker run --detach --name=mysql-ct -p 3306:3306 mysql:$(VERSION)

.PHONY:
load_data:
	docker build -t load:$(VERSION) -f ./load/Dockerfile .
	docker tag load:$(VERSION) $(REGISTRY)/load:$(TAG)
	docker push $(REGISTRY)/load:$(TAG)

.PHONY:
schema:
	docker build -t schema:$(VERSION) -f ./schema/Dockerfile .
	docker tag schema:$(VERSION) $(REGISTRY)/schema:$(TAG)
	docker push $(REGISTRY)/schema:$(TAG)

.PHONY:
run: mysql_init load_data schema
	argo submit -n argo --watch ../argo/garage_to_graph.yaml


.PHONY:
clean:
	- docker rm -f mysql-ct &> /dev/null
	- docker rmi mysql:$(VERSION)
	- docker rmi load:$(VERSION)
	- docker rmi $(REGISTRY)/load:$(TAG)
	- docker rmi schema:$(VERSION)
	- docker rmi $(REGISTRY)/schema:$(TAG)
