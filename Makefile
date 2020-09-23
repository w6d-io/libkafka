python:
	cargo build --features "python" --release
	cp -f ./target/release/libkafka.dylib ./kafka.so
	python tests/python_test.py

rust:
	cargo test
