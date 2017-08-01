## Camel Google BigQuery Component testing

The unit tests provided are somewhat limited.

Due to the nature of the component, it needs to be tested against a google BigQuery instance as no
emulator is available.

* Unit : <br>
  Standalone tests that can be conducted on their own
* Integration : <br>
  Tests against a Google BigQuery instance

### Execution of integration tests

A Google Cloud account with a configured BigQuery instance is required with a dataset created.

For the tests the `project.id` and `bigquery.datasetId` needs to be configured. By default
the current google user will be used to connect but credentials can be provided either by
account/key (via `service.account` and `service.key`) or a credentials file (`service.credentialsFileLocation`)

Running tests against BigQuery instance:

```
mvn -Pgoogle-bigquery-test verify
```


