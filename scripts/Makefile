release:
	cd ..; cargo build --release

docker:
	cd ..; cargo build --target=x86_64-unknown-linux-musl --release
	cd ..; mv target/x86_64-unknown-linux-musl/release/toshi scripts/toshi
	docker build -t zerosubnet/toshi .