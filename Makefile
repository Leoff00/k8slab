k := kubectl

build-api:
	docker build -t leoff00/simple-api:latest ./simple-api
	docker push leoff00/simple-api:latest

build-worker:
	docker build -t leoff00/simple-worker:latest ./simple-worker
	docker push leoff00/simple-worker:latest

apply:
	${k} apply -f k8s/$(args).yaml

.PHONY : build-api build-worker apply 