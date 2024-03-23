.PHONY: proto
proto:
	buf generate

.PHONY: clean
clean:
	rm -rf gen

.PHONY: test
test:
	gotestsum -- -coverprofile=cover.out ./...

.PHONY: integration
integration:
	env METAMORPHOSIS_INTEGRATION_TESTS=true go test -v ./integration_test.go

.PHONY: infra
infra:
	cd terraform && terraform apply -auto-approve

.PHONY: docker
docker:
	docker compose up

.PHONY: mocks
mocks:
	mockery --all --with-expecter=true --dir=.
