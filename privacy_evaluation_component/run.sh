cargo clean
docker build -t marshal .
docker run --rm --network host marshal cargo run -- homework preprocessed risks None None None