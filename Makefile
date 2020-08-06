
IMAGE?=registry.gitlab.com/limx0/deche:develop

docker-build:
	docker build -t ${IMAGE} .

docker-build-force:
	docker build -t ${IMAGE} --no-cache .

docker-push:
	docker push ${IMAGE}

docker-run:
	docker run -ti --entrypoint bash ${IMAGE}

docker-nebula:
	docker run -ti ${IMAGE} zeljko.betfair.nebula
