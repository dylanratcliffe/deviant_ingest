FROM golang:latest AS build

# Set necessary environmet variables needed for our image
ENV GOOS=linux \
    GOARCH=amd64 \
    CGO_ENABLED=0

WORKDIR $GOPATH/src/github.com/dylanratcliffe/deviant_dgraph

# Copy the code into the container
COPY . .

# Install dependencies
RUN go get -v -t && go mod vendor

# Build the application
RUN go build -o /go/bin/deviant_dgraph

# Build a small image
FROM alpine:latest

WORKDIR /deviant

COPY --from=build /go/bin/deviant_dgraph /deviant/deviant_dgraph

# Command to run
ENTRYPOINT ["/deviant/deviant_dgraph", "ingest", "--log", "debug"]