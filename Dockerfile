FROM rust:slim-buster AS builder

RUN apt-get update 
RUN apt-get install -y pkg-config libssl-dev
RUN rustup component add rustfmt

# Create appuser.
ENV USER=appuser
ENV UID=10001 
# See https://stackoverflow.com/a/55757473/12429735RUN 
RUN adduser \    
    --disabled-password \    
    --gecos "" \    
    --home "/nonexistent" \    
    --shell "/sbin/nologin" \    
    --no-create-home \    
    --uid "${UID}" \    
    "${USER}"

WORKDIR /app

COPY . .

RUN cargo build --release

# Use an unprivileged user.
USER appuser:appuser

# Port on which the service will be exposed.
EXPOSE 8080
# Run the uniqin binary.
ENTRYPOINT ["/app/target/release/toshi"]




