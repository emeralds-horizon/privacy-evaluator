cargo clean
docker build -t preprocessing .
docker run --rm --network host preprocessing cargo run -- example preprocessed [None,None,None,None,None] [0.05] [0.0005,10]