SHELL:=/bin/bash
_: mysql gremlin load schema



.PHONY: mysql
mysql: remove-mysql build-mysql run-mysql

build-mysql:
	docker build -t mysql:0.1 -f ./mysql/Dockerfile .

run-mysql:
	docker run --detach --name=mysql-ct --net=host mysql:0.1

remove-mysql:
	docker rm -f mysql-ct &> /dev/null



.PHONY: gremlin
gremlin: remove-gremlin build-gremlin run-gremlin

build-gremlin:
	docker build -t gremlin:0.1 -f ./gremlin/Dockerfile-gremlin-server .

run-gremlin:
	docker run --detach --name=gremlin-ct --net=host gremlin:0.1

remove-gremlin:
	docker rm -f gremlin-ct &> /dev/null



.PHONY: load
load: remove-load build-load run-load

build-load:
	docker build -t load:0.1 -f ./load/Dockerfile .

run-load:
	sleep 15 && docker run --detach --name=load-ct --net=host load:0.1

remove-load:
	docker rm -f load-ct &> /dev/null



.PHONY: schema
schema: remove-schema build-schema run-schema

build-schema:
	docker build -t schema:0.1 -f ./schema/Dockerfile .

run-schema:
	docker run --detach --name=schema-ct --net=host schema:0.1

remove-schema:
	docker rm -f schema-ct &> /dev/null



remove-all: remove-mysql remove-gremlin remove-load remove-schema