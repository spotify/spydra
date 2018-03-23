.DEFAULT_GOAL := build

IMAGE_NAME ?= <image>
IMAGE_TAG ?= <tag>
TAGGED_IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)
SPYDRA_VERSION ?= HEAD
SPYDRA_GIT_URL ?= ../../spydra
SPYDRA_TEST_BUCKET ?= <bucket>

K8S_NAMESPACE ?= default
K8S_POD_NAME ?= spydra-integration-tests

build:
	rm -rf spydra
	git clone ${SPYDRA_GIT_URL}
	bash -c "cd spydra && git checkout ${SPYDRA_VERSION}"
	docker build -t $(TAGGED_IMAGE) --label name="Spydra integration-tests image" .

publish:
	gcloud docker -- push $(TAGGED_IMAGE)

run:
	kubectl run --namespace=$(K8S_NAMESPACE) --image-pull-policy=Always --restart=Never --image=$(TAGGED_IMAGE) $(K8S_POD_NAME) -- -b $(SPYDRA_TEST_BUCKET)
	while [  $$(kubectl get po --namespace=$(K8S_NAMESPACE) | grep -E "$(K8S_POD_NAME).*ContainerCreating|Pending" | wc -l) -gt 0 ]; do sleep 1; done
	kubectl logs -f --namespace=$(K8S_NAMESPACE) $(K8S_POD_NAME)
