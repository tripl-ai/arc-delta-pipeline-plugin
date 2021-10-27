# Change Log

## 2.8.0

- bump to Delta 1.0.0.
- bump to Spark 3.1.2.

## 2.7.0

- bump to Delta 0.8.0.
- bump to Spark 3.0.2.

## 2.6.0

- catch `No commits found` errors from Delta in `DeltaLakeExtract`.

## 2.5.0

- bump to Arc 3.7.0.

## 2.4.1

- set name on Dataframe when `persist=true` to help understand persisted datasets when using the Spark UI.
- bump to Arc 3.4.1.

## 2.4.0

- bump to Arc 3.4.0.

## 2.3.1

- **FIX** pass through `numPartitions` to `DeltaLakeMerge` load properly.

## 2.3.0

- add `canReturnLastCommit` support for `relativeVersion` which will return the oldest version supported greater than `relativeVersion`.
- add `schemaURI` and `schemaView` to `DeltaLakeExtract` to allow provision of a schema if the `inputURI` does not exist.

## 2.2.0

- add `createTableIfNotExists` option to `DeltaLakeMergeLoad` to allow creation of initial set if missing. Default `false`.
- add logging of `operationMetrics` to all stages`.
- update to Arc 3.2.0.

## 2.1.0

- add snippets and documentation links to implement `JupyterCompleter`.

## 2.0.0

- update base `DeltaLake` version to `0.7.0`.
- update to Arc 3.0.0.
- add `partitionBy` and `numPartitions` to `DeltaLakeMergeLoad`.

## 1.9.0

- update base `DeltaLake` version to `0.6.0`.
- add `canReturnLastCommit` to `DeltaLakeExtract` `options` to allow `timestampAsOf` to return data when provided timestamp is greater than timestamp of last commit.

## 1.8.0

- update to Arc 2.10.0

## 1.7.0

- add the `DeltaLakeMergeLoad` stage. This currently relies on a custom `DeltaLake` build in `./lib` to support the `whenNotMatchedBySourceDelete` capability but a merge request has been raised against the DeltaLake offical repository.

## 1.6.0

- change default `overwriteSchema` to `true` (previously `false`) to ensure column metadata changes are persisted.

## 1.5.0

- add `generateSymlinkManifest` option (default `true`) to `DeltaLakeLoad` to generate a manifest file which allows Presto to read the DeltaLake written records.

## 1.4.1

- fix defect in `DeltaLakeLoad` to allow `options` to be specified.

## 1.4.0

- add `relativeVersion` in addition to `timestampAsOf` and `versionAsOf` options to allow users to specify a relative version where `0` is the latest and `-1` is the previous version.

## 1.3.0

- add logging of version/timestamp to `DeltaLakeExtract` and `DeltaLakeLoad`.

## 1.2.0

- bump to [Delta Lake 0.4.0](https://github.com/delta-io/delta/releases/tag/v0.4.0)
- update to Arc 2.1.0
- update to Scala 2.12.10

## 1.1.1

- update to Spark 2.4.4
- update to Arc 2.0.1
- update to Scala 2.12.9

## 1.1.0

- bump to [Delta Lake 0.3.0](https://github.com/delta-io/delta/releases/tag/v0.3.0)

## 1.0.0

- initial release.
