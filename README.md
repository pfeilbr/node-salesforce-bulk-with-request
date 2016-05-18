# node-salesforce-bulk-with-request

salesforce bulk api client library using [request](https://github.com/request/request) module

## Running

```sh
// run and capture all output to tmp/log.txt
DEBUG=BulkClient npm test -- --no-timeouts > tmp/log.txt  2>&1
```

## Tests

```sh
$ npm test

// live / watch 
$ npm test -- -w --no-timeouts
```

### Resources

- [Bulk API Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/)