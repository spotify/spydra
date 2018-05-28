# Run integration tests on k8s

This is for running the integrations tests on a k8s pod, with applicaiton default credentials.

Example usage:

```
IMAGE_NAME=us.gcr.io/my-registry/spydra/spydra-integration-test \
  IMAGE_TAG=-test-1 \
  SPYDRA_TEST_BUCKET=my-bucket-for-spydra-integration-tests \
  K8S_NAMESPACE=default \
  K8S_POD_NAME=spydra-integration-tests \
  make build publish run
```
