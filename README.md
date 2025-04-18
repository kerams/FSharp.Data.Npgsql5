## Description
FSharp.Data.Npgsql5 is an F# type provider library built on top of [Npgsql ADO.NET client library]( http://www.npgsql.org/doc/index.html).

Forked from https://github.com/demetrixbio/FSharp.Data.Npgsql, ported to Npgsql 6+ with a bunch of improvements and some breaking changes. Credits to original authors and contributors.

## Nuget package
FSharp.Data.Npgsql5 [![Nuget](https://img.shields.io/nuget/v/FSharp.Data.Npgsql5.svg?colorB=green)](https://www.nuget.org/packages/FSharp.Data.Npgsql5)
 
## Setup
All examples are based on the [DVD rental sample database](https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/) and assume the following definitions exist:
```fsharp
[<Literal>]
let dvdRental = "Host=localhost;Username=postgres;Database=dvdrental"

open FSharp.Data.Npgsql

type DvdRental = NpgsqlConnection<dvdRental>
let dataSource = Npgsql.NpgsqlDataSource.Create dvdRental
```

## Basic query

```fsharp
let printMovies () = task {
    use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dataSource)
    let! movies = cmd.TaskAsyncExecute()

    for x in movies do   
        printfn "Movie '%s' released in %i." x.title x.release_year.Value
```

## Parameterized query

```fsharp
use cmd = DvdRental.CreateCommand<"SELECT title FROM public.film WHERE length > @longer_than">(dataSource)
let longerThan = TimeSpan.FromHours(3.)
let xs: string list = cmd.TaskAsyncExecute(longer_than = int16 longerThan.TotalMinutes).Result |> Seq.toList 
printfn "Movies longer than %A:\n%A" longerThan xs
```

## Retrieve singleton record
Set `SingleRow = true` to retrieve a single row as an option instead of a collection of rows. `TaskAsyncExecute` will throw if the result set contains more than one row.

```fsharp
use cmd = DvdRental.CreateCommand<"SELECT current_date as today", SingleRow = true>(dataSource)
cmd.TaskAsyncExecute().Result |> printfn "Today is: %A"
```

## Result types

There are 4 result types:
 - `ResultType.Record` (default) - returns a record-like class with read-only properties.  See examples above. 
 - `ResultType.Tuples` - returns a tuple whose elements represent row's columns.
 
 ```fsharp
 use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3", ResultType.Tuples>(dataSource)
 for title, releaseYear in cmd.TaskAsyncExecute().Result do   
     printfn "Movie '%s' released in %i." title releaseYear.Value
 ```
 - `ResultType.DataTable` - comes in handy when you need to do updates, deletes or upserts. For insert only ETL-like workloads use statically typed data tables. See [Data modifications](#data-modifications) section for details. 
 - `ResultType.DataReader` - returns a plain `NpgsqlDataReader`. You can pass it to [DataTable.Load](https://docs.microsoft.com/en-us/dotnet/api/system.data.datatable.load?view=netstandard-2.0) for merge/upsert scenarios.

## Collection types

You can customize the type of collection commands return globally on `NpgsqlConnection` and override it on each `CreateCommand`. This setting is ignored when combined with `SingleRow = true` (`option` is used instead of a collection). You can choose between 4 collections:
- `CollectionType.List` (default)
- `CollectionType.Array`
- `CollectionType.ResizeArray`
- `CollectionType.LazySeq` - A special type whose `Seq` property allows you to lazily iterate over the query results. In other words, results are not materialized on the server (useful when you want to process a lot of data without loading it all into memory at once), but are only retrieved from Postgres on demand. You need to **make sure to dispose of the `LazySeq` instance** (instead of the command) to avoid dangling Npgsql connections.
  - May only be used when the command returns one result set.
  - May only be enumerated once. If this is a problem, you should consider using a different collection type, because being able to enumerate the sequence repeatedly implies having the results materialized, which defeats the primary purpose of `LazySeq`.

```fsharp
let doStuff = task {
    use cmd = DvdRental.CreateCommand<"SELECT * from film limit 5", CollectionType = CollectionType.Array>(dataSource)
    let! actual = cmd.TaskAsyncExecute() // this is an array instead of list now
    actual |> Array.iter (printfn "%A") }
```

```fsharp
let lazyData () =
    use cmd = DvdRental.CreateCommand<"SELECT * from generate_series(1, 1000000000)", CollectionType = CollectionType.LazySeq>(dataSource)
    cmd.TaskAsyncExecute().Result

let doStuff () =
    use data = lazyData ()
    // Only one million instead of the billion generated rows is transferred from Postgres
    // These rows are not materialized at once either but loaded into memory
    // and then released one by one (or a bit more depending on prefetch in Npgsql) as they are processed
    data.Seq |> Seq.take 1_000_000 |> Seq.iter (printfn "%A")
```

## Reuse of provided records

By default in **v2**, every `CreateCommand` generates a completely seperate type when using `ResultType.Record`. This can be annoying when you have similar queries that return the same data structurally and you cannot, for instance, use one function to map the results onto your domain model.
`NpgsqlConnection` exposes the static parameter `ReuseProvidedTypes` to alleviate this issue. When set to true, all commands that return the same columns (**column names and types must match exactly**, while select order does not matter) end up sharing the same provided record type too. Starting from **v3**, provided types are always reused.
The following snippet illustrates how you could reuse a single function to map the result of 2 distinct queries onto the `Film` type:

```fsharp
type DvdRental = NpgsqlConnection<dvdRental, ReuseProvidedTypes = true>

type Film = { Title: string; Rating: DvdRental.``public``.Types.mpaa_rating option }

// CreateCommand returning a type we want to refer to in a function signature has to be 'mentioned' first
let getAllFilmsWithRatingsCommand = DvdRental.CreateCommand<"select title, rating from film">

// The type with title and rating is now generated and accessible
let mapFilm (x: DvdRental.``rating:Option<public.mpaa_rating>, title:String``) =
    { Title = x.title; Rating = x.rating }
	
let getAllFilmsWithRatings () =
    use cmd = getAllFilmsWithRatingsCommand dataSource
    let res = cmd.TaskAsyncExecute().Result
    res |> List.map mapFilm
	
let getFilmWithRatingById id =
    use cmd = DvdRental.CreateCommand<"select title, rating from film where film_id = @id", SingleRow = true>(dataSource)
    let res = cmd.TaskAsyncExecute(id).Result
    res |> Option.map mapFilm
```

## Naming 

The type provider's `NpgsqlConnection` may clash with `NpgsqlConnection` defined in Npgsql. If you end up having the following error message:

```
FS0033	The non-generic type 'Npgsql.NpgsqlConnection' does not expect any type arguments, but here is given 2 type argument(s)	
```

It means that `Npgsql.NpgsqlConnection` shadowed `FSharp.Data.Npgsql.NpgsqlConnection` because ```open FSharp.Data.Npgsql``` statement was followed by ```open Npgsql```.

There are several ways to work around the issue:

 - Use fully qualified names for the type provider. For example:

```fsharp
type DvdRental = FSharp.Data.Npgsql.NpgsqlConnection<connectionString>
```

 - Use a fully qualified name for `Npgsql.NpgsqlConnection`

 - Use a type alias for `Npgsql.NpgsqlConnection`
```fsharp
type PgConnection = Npgsql.NpgsqlConnection
```

## Prepared statements
[Prepared statements](https://www.npgsql.org/doc/prepare.html) are supported by setting the static parameter `Prepare` to `true`. For `NpgsqlConnection` this can be set when defining the type itself and also overriden when calling `CreateCommand`.

```fsharp
// All commands created from this type will be prepared
type DvdRental = NpgsqlConnection<dvdRental, Prepare = true>

// Will be prepared
use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3">(dataSource)
for x in cmd.TaskAsyncExecute().Result do   
    printfn "Movie '%s' released in %i." x.title x.release_year.Value

// Overrides the DvdRental setting and thus won't be prepared
use cmd = DvdRental.CreateCommand<"SELECT title, release_year FROM public.film LIMIT 3", Prepare = false>(dataSource)
for x in cmd.TaskAsyncExecute().Result do   
    printfn "Movie '%s' released in %i." x.title x.release_year.Value
```

## Data modifications
- Hand-written statements
```fsharp
//deactivate customer if exists and active
let email = "mary.smith@sakilacustomer.org"

use cmd = DvdRental.CreateCommand<"
    UPDATE public.customer
    SET activebool = false
    WHERE email = @email
	AND activebool
", SingleRow = true>(dataSource)

let recordsAffected = cmd.TaskAsyncExecute(email).Result
if recordsAffected = 0
then
printfn "Could not deactivate customer %s" email
elif recordsAffected = 1
then
use restore =
    DvdRental.CreateCommand<"
	UPDATE public.customer
	SET activebool = true
	WHERE email = @email
    ">(dataSource)
assert( restore.TaskAsyncExecute(email).Result = 1)
```
- `ResultType.DataTable` - good to handle updates, deletes, upserts or inserts mixed with any above. 

```fsharp
//Deactivate customer if found and active
use conn = dataSource.OpenConnection ()
use tx = conn.BeginTransaction ()
use cmd =
DvdRental.CreateCommand<"
    SELECT customer_id, activebool
    FROM public.customer
    WHERE email = @email
", ResultType.DataTable>(conn, tx)
let t = cmd.TaskAsyncExecute(email = "mary.smith@sakilacustomer.org").Result
if t.Rows.Count > 0 && t.Rows.[0].activebool
then
t.Rows.[0].activebool <- true
assert( t.Update(conn, tx) = 1)

//Commit to persist changes
//tx.Commit()
```

- Statically-typed data tables for inserts-only scenarios (for example ETL).

```fsharp
use conn = dataSource.OpenConnection ()
use tx = conn.BeginTransaction ()
let t = new DvdRental.``public``.Tables.actor()

let r = t.NewRow(first_name = "Tom", last_name = "Hanks")
t.Rows.Add(r)

//or
//t.AddRow(first_name = "Tom", last_name = "Hanks")
//let r = t.Rows.[0]

assert( t.Update(conn, tx) = 1)
printfn "Identity 'actor_id' %i and column with default 'last update': %A auto-fetched." r.actor_id r.last_update
```
It's worth noting that statically typed tables know to auto-fetch generated IDs and default values after insert, but only if `updateBatchSize` is set to 1 (default). 

## Transactions
  
In order to use transactions across commands, pass the `XCtor = true` static parameter to `CreateCommand` so that the method signature accepts a connection + optional transaction. `XCtor` stands for extended constructor.

```fsharp
use conn = dataSource.OpenConnection ()
use tx = conn.BeginTransaction ()
use cmd = 
DvdRental.CreateCommand<"        
    INSERT INTO public.actor (first_name, last_name)
    VALUES(@firstName, @lastName)`
", XCtor = true>(conn, tx)
assert(cmd.TaskAsyncExecute(firstName = "Tom", lastName = "Hanks").Result = 1)
//Commit to persist changes
//tx.Commit()
```
`XCtor` also can be set on top level effectively making all `CreateCommand` methods to accept connection + transaction combination. 
```fsharp
type DvdRentalXCtor = NpgsqlConnection<dvdRental, XCtor = true>

use conn = dataSource.OpenConnection ()
use tx = conn.BeginTransaction ()
use cmd =
DvdRentalXCtor.CreateCommand<"
    INSERT INTO public.actor (first_name, last_name)
    VALUES(@firstName, @lastName)
">(conn, tx)
assert(cmd.TaskAsyncExecute(firstName = "Tom", lastName = "Hanks").Result = 1)
//Commit to persist changes
//tx.Commit()
```

## Optional input parameters
By default all input parameters of `TaskAsyncExecute` methods generated by the type provider are mandatory. There are rare cases when you prefer to handle NULL input values inside SQL script. `AllParametersOptional` set to true makes all parameters optional.
```fsharp
use cmd = new NpgsqlCommand<"
SELECT coalesce(@x, 'Empty') AS x
", dvdRental, AllParametersOptional = true, SingleRow = true>(dataSource)

assert( cmd.TaskAsyncExecute(Some "test").Result = Some( Some "test"))
assert( cmd.TaskAsyncExecute().Result = Some( Some "Empty")) 
```

## Bulk Copy
To upload a large amount of data fast, use the `BinaryImport` method on statically typed data tables:
```fsharp
let firstName, lastName = "Tom", "Hanks"
use conn = dataSource.OpenConnection ()
use tx = conn.BeginTransaction ()

let actors = new DvdRental.``public``.Tables.actor()

let actor_id =
use cmd = DvdRental.CreateCommand<"select nextval('actor_actor_id_seq' :: regclass)::int", SingleRow = true, XCtor = true>(conn, tx)
cmd.TaskAsyncExecute().Result |> Option.flatten

//Binary copy operation expects all columns including auto-generated and having defaults to be populated.
//Therefore we must provide values for actor_id and last_update columns which are optional for plain Update method.
actors.AddRow(actor_id, first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now)

let rowsImported = actors.BinaryImport(conn, false, System.Threading.CancellationToken.None).Result

use cmd =
DvdRental.CreateCommand<
    "SELECT COUNT(*) FROM public.actor WHERE first_name = @firstName AND last_name = @lastName",
    SingleRow = true,
    XCtor = true>(conn, tx)

assert(Some( Some 1L) = cmd.TaskAsyncExecute(firstName, lastName).Result)
```

If you're importing data to a table with an identity column and want the database to generate those values for you, you can instruct `BinaryImport` not to fill in identity columns with `actors.BinaryImport(conn, true)`.

## PostGIS
PostGIS columns and input parameters of type `geometry` are supported using [NetTopologySuite](https://github.com/NetTopologySuite/NetTopologySuite).

```fsharp
open NetTopologySuite.Geometries

let input = Geometry.DefaultFactory.CreatePoint (Coordinate (55., 0.))
use cmd = DvdRental.CreateCommand<"select @p::geometry">(dataSource)
let res = cmd.TaskAsyncExecute(input).Result.Head.Value
    
Assert.Equal (input.Coordinate.X, res.Coordinate.X)
```

If you intend to use `geometry`, you have to register the type handler in the data source.

```fsharp
open type Npgsql.NpgsqlNetTopologySuiteExtensions

let dataSource =
    let x = Npgsql.NpgsqlDataSourceBuilder connectionString
    x.UseNetTopologySuite () |> ignore
    x.Build ()
```

## Limitations

  - One unfortunate PostgreSQL limitation is that column nullability cannot be inferred for derived columns. A command 
  ```fsharp
  use cmd = DvdRental.CreateCommand<"SELECT 42 AS Answer">(dataSource)
  assert( cmd.TaskAsyncExecute().Result |> Seq.exactlyOne = Some 42)
  ```
  will infer `seq<Option<int>>` as the result although it cleary should be `seq<int>`. 
  - Data modification batch processing is [not supported](https://github.com/npgsql/npgsql/issues/1830).

## Running tests
From the repo root folder
- dotnet build .\src\DesignTime\
- dotnet build .\src\Runtime\
- docker build -t pg_dvdrental .\tests\
- docker run -d -p 5432:5432 --name dvdrental pg_dvdrental
- dotnet test .\tests\

## Releasing package

(Notes to self)
```
cd src/Design
dotnet build -c Release
cd ../Runtime
dotnet build -c Release
vi paket.template 
bump version number
paket pack
paket push FSharp.Data.Npgsql.<versionNumber>.nupkg --api-key <insert api key>
```
