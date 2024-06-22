.PHONY: proto
proto:
	buf generate

.PHONY: clean
clean:
	rm -rf gen

.PHONY: test
test:
	gotestsum -- -coverprofile=cover.out github.com/binarymatt/metamorphosis

.PHONY: integration
integration:
	env METAMORPHOSIS_INTEGRATION_TESTS=true gotestsum -- -v ./...

.PHONY: infra
infra:
	cd terraform && terraform apply -auto-approve

.PHONY: docker
docker:
	docker compose up

.PHONY: mocks
mocks:
	mockery --all --with-expecter=true --dir=.
