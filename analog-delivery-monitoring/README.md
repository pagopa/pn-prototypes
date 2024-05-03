# Analog Delivery Monitoring

### Description
This folder contains all queries that allow analog delivery monitoring creating
a queryable layer we can use to build:
- ad-hoc analysis
- repeatable reports

### How to

#### Views from source
Every new view that holds data from any source (like S3) must be saved
within the `/source-views` folder, with a name that follows this
nomenclature: `<table_name | file_name>.sql`.

#### Logical views
Every new calculated view that holds manipulated data must be saved
within the `/logical-views` folder, in order to divide every single logical view
and re-use them when necessary by keeping file small and readable.

Every file name must be compliant with the name of the new logical view.

#### Computational query
Every query that return one or more results, must be saved 
within the `/queries` folder.

#### General notes
It can be very helpful putting in each SQL file the dependencies on other
SQL file, like views, in order to make clean and clear what is the scope
of the query.

Example: `
DEPENDENCIES:
/source-views/pnTimelines.sql
/source-views/pnEcRichiesteMetadati.sql 
/source-views/matriceCosti2023Pivot.sql
`