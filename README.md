# Dgraph

This project stores items from a NATS network into dgraph.

## Running

```shell
go run main.go ingest
```

## Viewing results

Open the [Ratel dashboard](http://localhost:8000/?latest) and run whatever query you want. Examples:

Get all items:

```
{
  all(func: type(Item)) {
    uid
    expand(_all_) {
      expand(_all_)
    }
  }
}
```

## Developing

All code should be developed in a feature branch and pull requests must be approved by a maintainer.

### Running Tests

To run spec tests run:

```shell
go test -v ./... -short
```

To run acceptance tests also:

```shell
go test -v ./...
```

Supporting infrastructure for acceptance tests can be created by running:

```shell
docker-compose up
```