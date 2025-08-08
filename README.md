# BioNews Data Warehouse

This repository contains a [Dataform](https://cloud.google.com/dataform/docs) model that processes, transforms, aggregates, and outputs Google Analytics 4 (GA4) and Google Ad Manager (GAM) data.

## Installing the model

[TODO](docs/local-dev-setup.md)

## Running the model

TODO
```sh
npm install
dataform compile
datafor run --dry-run
```

## Sources

### Google Analytics 4 (GA4)

GA4 data is exported to BigQuery using the standard export.  This data follows the [event](https://support.google.com/analytics/answer/7029846?hl=en) and [user](https://support.google.com/analytics/answer/12769371?hl=en&ref_topic=9359001&sjid=7895718207062367276-NA) schemas.  Daily tables will usually begin to be available in the morning in the Eastern timezone.  Intraday tables that contain realtime data can be found for the current day.


### Google Ad Manager (GAM)

[GAM data](https://support.google.com/admanager/answer/1733124) is configured via the [BigQuery Data Transfer service](https://cloud.google.com/bigquery/docs/doubleclick-publisher-transfer), though the data must be enabled by contacting [TODO].  These tables are currently exported:

- `NetworkImpressions` (and `NetworkBackfillImpressions`) - "Information about downloaded impressions."
- `NetworkClicks` (and `NetworkBackfillClicks`) - "Information about clicks."

 #### [+ Adding a new data source](docs/add-new-source.md)

 ## Transformations

 GA4 and GAM data is processed and transformed in several ways:

 - The GA4 models UNENST event parameters into new columns, aggregate event data to the session and user level and identify conversions.
 - GAM table columns are renamed to `snake_case`, timestamp integers are converted to actual TIMESTAMP fields, and `key=value` pairs are extracted from the `CustomTargeting` field (including `pvid`).
 - GA4 and GAM data are joined on `pvid`.  This allows us to roll up GAM events (ad clicks and impressions) to a GA4 session and user ID.


