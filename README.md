# iceberg-go-cli

This project provides a small CLI which allows you to view the contents of an Iceberg repository.

# Usage

Once you have created an iceberg table in AWS, personally I use Athena to create this table, you can run a the CLI as follows providing the Glue database name, and table name.

```
go run main.go --database myapp_prod --table cloudfront_logs
```

This should retrieve the current manifest, then print out the schema, sort order, and current manifests.

# License

This application is released under Apache 2.0 license and is copyright [Mark Wolfe](https://www.wolfe.id.au/?utm_source=action-workflow-check).