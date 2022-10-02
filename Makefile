.PHONY: fmt
fmt:
	find -type f -name "*.rs" -not -path "*target*" -exec rustfmt --edition 2021 {} \;
	
.PHONY: lint
lint:
	cargo +nightly clippy --fix -Z unstable-options --release --all

.PHONY: build-cli
build-cli:
	(./scripts/build_cli.sh yes)

.PHONY: build-cli-debug
build-cli-debug:
	(./scripts/build_cli.sh no)
