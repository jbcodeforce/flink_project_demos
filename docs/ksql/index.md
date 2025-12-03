# KSQL Tutorial Queries

This project includes the [ksql queries from Confluent tutorials](https://developer.confluent.io/tutorials/) to be used as a source for automatic migration testing.

The [ksql_tutorial](https://github.com/jbcodeforce/flink_project_demos/tree/main/ksql_tutorial) folder is by tutorial subjects

## Routing

Deciding where to send events, splitting, merging, and filtering streams.

Folder `ksql_tutorial/sources/routing`:

* [Filtering- Splitting](https://developer.confluent.io/confluent-tutorials/splitting/ksql/) `acting_events` to acting_events_fantasy, acting_events_other -> file `splitting.ksql`
* [Merge multiple streams](https://developer.confluent.io/confluent-tutorials/merging/ksql/) into one `all_songs` stream -> file `merge.ksql`
* [Filtering streams](https://developer.confluent.io/confluent-tutorials/filtering/ksql/) from `all_publications` -> file `filtering.ksql`
* [Deduplication](https://developer.confluent.io/confluent-tutorials/deduplication-windowed/ksql/) clicks events within a time window -> file `deduplicate.ksql`


## Aggregations

Under `ksql_tutorial/sources/aggregations` folder 

* [Count messages](https://developer.confluent.io/confluent-tutorials/count-messages/ksql/) from pageviews -> file `count_pageviews.ksql`

## Joins 

* [Join stream to stream](https://developer.confluent.io/confluent-tutorials/joining-stream-stream/ksql/) join two event streams on a common key in order to create a new enriched event stream.


## Flink References

The `ksql_tutorial/flink_ref` folder includes the matching Flink SQL references, migrated using the `shift_left` tool, with some tuning, and pipeline organization.

### Current Migration Status

| <div style="width:100px">KSQL | Flink SQL | Validated on CC | Validated on CP Flink | Note |
| ---- | --------- | :-------------: | :-------------------: | ---- |
| routing/splitting.ksql | sources/acting_events/acting_events | ✅ | ❌ | test data / deduplicates|
| | dimensions/acting_events/acting_events_drama | ✅ | ❌ | Data results ok|
| | dimensions/acting_events/acting_events_fantasy | ✅ | ❌ | Data results ok|
| | dimensions/acting_events/acting_events_other | ✅ | ❌ | Data results ok|
| routing/merge.ksql | sources/songs/classical |  ✅ | ❌ | test data / deduplicates|
|  | sources/songs/rock | ✅ | ❌ | test data / deduplicates|
| | dimensions/songs/all_songs | ✅ | ❌ | Data results ok|
| routing/deduplicate.ksql | | | |
| routing/filtering.ksql | | | |
| aggregations/count_pageviews.ksql | sources/web/pageviews | | |
| joins/stream_stream.ksql | sources/web/pageviews | | |

???- "Example of pipeline table creation"
    Below are some examples of table creations done with `shift_left table init`
    ```sh
    shift_left table init acting_events $PIPELINES/sources --product-name acting_events
    shift_left table init acting_events_other $PIPELINES/sources --product-name acting_events 
    shift_left table init acting_events_fantasy $PIPELINES/sources --product-name acting_events

    shift_left table init classical  $PIPELINES/sources --product-name songs
    ```

## Some migrations dry run

For a complete tutorial on migration using AI see [this note](https://jbcodeforce.github.io/shift_left_utils/tutorial/migration_ai_lab/)

Use the `run_migration.sh`

* Basic usage
    ```sh
    ./run_migration.sh all_songs sources/routing/merge.ksql
    ```

* With custom staging directory
    ```sh
    STAGING=/custom/path ./run_migration.sh pageviews_count sources/aggregations/count_pageviews.ksql
    ```

* Show help
    ```sh
    ./run_migration.sh --help
    ```
