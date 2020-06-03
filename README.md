# spark-cdm

This repo is now **obsolete** and will not be updated further. A new Spark CDM Connector library is now available here: https://github.com/Azure/spark-cdm-connector

A prototype Spark data source for the Azure "Common Data Model". Reading and writing is supported, but spark-cdm is definitely a work in progress. Please file issues for any bugs that you find. For more information about the Azure Common Data Model, check out [this page](https://docs.microsoft.com/en-us/common-data-model/data-lake). <br>

## Example

1. Create an AAD app and give the service principal the "Storage Blob Data Contributor" role on the ADLSgen2 storage account used for your CDM data.
2. Install the JAR in the `release` directory in this repo on your Spark cluster.
3. Check out the below code for basic read and write examples.

```scala
val df = spark.read.format("com.microsoft.cdm")
                .option("cdmModel", "https://YOURADLSACCOUNT.dfs.core.windows.net/FILESYSTEM/path/to/model.json")
                .option("entity", "Query")
                .option("appId", "YOURAPPID")
                .option("appKey", "YOURAPPKEY")
                .option("tenantId", "YOURTENANTID")
                .load()

// Do whatever spark transformations you want
val transformedDf = df.filter(...)

// entity: name of the entity you wish to write.
// modelDirectory: writes to a model.json file located at the root of this directory. note: if there is already a model.json in this directory, we will append an entity to it
// modelName: name of the model to write. N/A in append case for now.
transformedDf.write.format("com.microsoft.cdm")
            .option("entity", "FilteredQueries")
            .option("appId", "YOURAPPID")
            .option("appKey", "YOURAPPKEY")
            .option("tenantId", "YOURTENANTID")
            .option("cdmFolder", "https://YOURADLSACCOUNT.dfs.core.windows.net/FILESYSTEM/path/to/output/directory/")
            .option("cdmModelName", "MyData")
            .save()
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
