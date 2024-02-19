FROM rust:1.72

WORKDIR /usr/src/myapp
COPY . .

RUN cargo install --path .

CMD ["myapp"]