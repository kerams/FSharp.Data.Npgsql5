module NpgsqlConnectionTests

open System
open Xunit
open FSharp.Data.Npgsql
open System.Reflection
open System.Threading
open type Npgsql.NpgsqlNetTopologySuiteExtensions
open NetTopologySuite.Geometries

#nowarn 44

type SimpleComposite () =
    member val SomeArray: int[] = null with get, set
    member val SomeText: string = null with get, set
    member val SomeNumber = 0L with get, set

let isStatementPrepared (connection: Npgsql.NpgsqlConnection) =
    // npgsql 7
    let pool = typeof<Npgsql.NpgsqlConnection>.GetField("_dataSource", BindingFlags.NonPublic ||| BindingFlags.Instance).GetValue(connection)

    let connectors = pool.GetType().GetField("Connectors", BindingFlags.NonPublic ||| BindingFlags.Instance).GetValue(pool) :?> obj[]

    let mutable count = 0

    for connector in connectors do
        if isNull connector |> not then
            let psManager = connector.GetType().GetProperty("PreparedStatementManager", BindingFlags.NonPublic ||| BindingFlags.Instance).GetValue(connector)
            let preparedStatements = psManager.GetType().GetProperty("BySql", BindingFlags.NonPublic ||| BindingFlags.Instance).GetValue(psManager)
            count <- preparedStatements.GetType().GetProperty("Count", BindingFlags.Public ||| BindingFlags.Instance).GetMethod.Invoke(preparedStatements, [||]) :?> int + count

    count > 0

[<Literal>]
let selectFromPartitionedTable = "select * from logs where log_time between '2019-01-01' and '2019-12-31'"

[<Literal>]
let selectFromSpecificPartition = "select * from logs_2019 where log_time between '2019-01-01' and '2019-12-31'"

[<Literal>]
let getActorsAndFilms = "select * from actor limit 5; select * from film limit 5"

[<Literal>]
let getActorsUpdateActorsGetFilms = "select * from actor limit 5; update actor set actor_id = 1 where actor_id = 1; select * from film limit 5"

[<Literal>]
let fourSelects = "SELECT * FROM generate_series(1, 10); SELECT * FROM generate_series(1, 10); SELECT * FROM generate_series(1, 10); SELECT * FROM generate_series(1, 10)"

[<Literal>]
let updateActorsUpdateSelectActorsUpdateFilms = "update actor set actor_id = actor_id where actor_id in (2, 3); select * from actor limit 5; update film set film_id = 1 where film_id = 1"

[<Literal>]
let getActorByName = "
    SELECT first_name, last_name
    FROM public.actor 
    WHERE first_name = @firstName AND last_name = @lastName "

[<Literal>]
let connectionString = "Host=localhost;Username=postgres;Password=postgres;Database=dvdrental;Port=5432"

let ds =
    let x = Npgsql.NpgsqlDataSourceBuilder connectionString
    x.UseNetTopologySuite().MapComposite<SimpleComposite> "simple_type" |> ignore
    x.Build ()

let openConnection () = 
    ds.OpenConnection ()

type DvdRental = NpgsqlConnection<connectionString>

[<Fact>]
let ``Multi-statement command with SingleRow, no result`` () =
    use cmd = DvdRental.CreateCommand<"update actor set first_name = first_name; select id from logs where false", SingleRow = true>(ds)
    let res = cmd.TaskAsyncExecute().Result
    Assert.Equal (200, res.RowsAffected1)
    Assert.Equal (None, res.ResultSet2)

[<Fact>]
let selectLiterals() =
    use cmd =
        DvdRental.CreateCommand<"        
            SELECT 42 AS Answer, current_date as today 
        ">(ds)

    let x = cmd.TaskAsyncExecute().Result |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer)
    Assert.Equal(Some DateTime.Now.Date, x.today)

[<Fact>]
let selectSingleRow() =
    // per https://www.postgresql.org/docs/12/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT
    // CURRENT_TIME and CURRENT_TIMESTAMP deliver values with time zone
    use cmd = DvdRental.CreateCommand<"        
        SELECT 42 AS Answer, current_date as today
    ", SingleRow = true>(ds)

    Assert.Equal(
        Some( Some 42, Some DateTime.Now.Date), 
        cmd.TaskAsyncExecute().Result |> Option.map ( fun x ->  x.answer, x.today )
    )

[<Fact>]
let selectSingleRow2ResultSets () =
    use cmd = DvdRental.CreateCommand<"
        select 42;
        select 'nope'", SingleRow = true>(ds)

    let res = cmd.TaskAsyncExecute().Result

    Assert.Equal (Some 42, res.ResultSet1.Value)
    Assert.Equal (Some "nope", res.ResultSet2.Value)

[<Fact>]
let selectSingleNull() =
    use cmd = DvdRental.CreateCommand<"SELECT NULL", SingleRow = true>(ds)
    Assert.Equal(Some None, cmd.TaskAsyncExecute().Result)

[<Fact>]
let selectSingleColumn() =
    use cmd = DvdRental.CreateCommand<"SELECT * FROM generate_series(0, 10)">(ds)

    Assert.Equal<_ seq>(
        { 0 .. 10 }, 
        cmd.TaskAsyncExecute().Result |> Seq.choose id 
    )

[<Fact>]
let ``selectSingleColumn tuple`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * FROM generate_series(0, 10)", ResultType = ResultType.Tuples>(ds)

    Assert.Equal<_ seq>(
        { 0 .. 10 }, 
        cmd.TaskAsyncExecute().Result |> Seq.choose id 
    )

[<Fact>]
let paramInFilter() =
    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM generate_series(0, 10) AS xs(value) WHERE value % @div = 0
        ">(ds)

    Assert.Equal<_ seq>(
        { 0 .. 2 .. 10 }, 
        cmd.TaskAsyncExecute(div = 2).Result |> Seq.choose id 
    )

[<Fact>]
let paramInLimit() =
    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM generate_series(0, 10) LIMIT @limit
        ">(ds)

    let limit = 5
    Assert.Equal<_ seq>(
        { 0 .. 10 } |> Seq.take limit , 
        cmd.TaskAsyncExecute(int64 limit).Result |> Seq.choose id
    )

[<Literal>]
let getRentalById = "SELECT return_date FROM rental WHERE rental_id = @id"

[<Fact>]
let dateTableWithUpdate() =

    let rental_id = 2

    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM rental WHERE rental_id = @rental_id
        ", ResultType.DataTable>(ds) 
        
    let t = cmd.TaskAsyncExecute(rental_id).Result
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date
    let mutable rowsAffected = 0
    try
        let new_return_date = Some DateTime.Now.Date
        r.return_date <- new_return_date
        rowsAffected <- t.Update(ds)
        Assert.Equal(1, rowsAffected)

        use cmd = DvdRental.CreateCommand<getRentalById>(ds)
        Assert.Equal( new_return_date, cmd.TaskAsyncExecute( rental_id).Result |> Seq.exactlyOne ) 

    finally
        if rowsAffected = 1
        then 
            r.return_date <- return_date
            t.Update(ds) |>  ignore      
            
[<Fact>]
let dateTableWithUpdateAndTx() =
    
    let rental_id = 2
    
    use conn = openConnection()
    use tran = conn.BeginTransaction()

    use cmd = 
        DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", ResultType.DataTable, XCtor = true>(conn, tran)    
    let t = cmd.TaskAsyncExecute(rental_id).Result
    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    let return_date = r.return_date

    let new_return_date = Some DateTime.Now.Date
    r.return_date <- new_return_date
    Assert.Equal(1, t.Update(conn, transaction = tran))

    use getRentalByIdCmd = DvdRental.CreateCommand<getRentalById, XCtor = true>(conn, tran)
    Assert.Equal( 
        new_return_date, 
        getRentalByIdCmd.TaskAsyncExecute( rental_id).Result |>  Seq.exactlyOne
    ) 

    tran.Rollback()

    Assert.Equal(
        return_date, 
        DvdRental.CreateCommand<getRentalById>(ds).TaskAsyncExecute( rental_id).Result |> Seq.exactlyOne
    ) 

[<Fact>]
let dateTableWithUpdateWithConflictOptionCompareAllSearchableValues() =
    
    let rental_id = 2
    
    use conn = openConnection()
    use tran = conn.BeginTransaction()

    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * FROM rental WHERE rental_id = @rental_id
        ", ResultType.DataTable, XCtor = true>(conn, tran)    
  
    let t = cmd.TaskAsyncExecute(rental_id).Result

    [ for c in t.Columns ->  c.ColumnName, c.DataType, c.DateTimeMode  ] |> printfn "\nColumns:\n%A"

    Assert.Equal(1, t.Rows.Count)
    let r = t.Rows.[0]
    r.return_date <- r.return_date |> Option.map (fun d -> d.AddDays(1.))
    //Assert.Equal(1, t.Update(connection = conn, transaction = tran, conflictOption = Data.ConflictOption.CompareAllSearchableValues ))
    Assert.Equal(1, t.Update(conn, tran, conflictOption = Data.ConflictOption.OverwriteChanges ))
     
    use getRentalByIdCmd = DvdRental.CreateCommand<getRentalById, XCtor = true>(conn, tran)
    Assert.Equal( 
        r.return_date, 
        getRentalByIdCmd.TaskAsyncExecute( rental_id).Result |>  Seq.exactlyOne 
    ) 

[<Fact>]
let deleteWithTx() =
    let rental_id = 2

    let cmd () = DvdRental.CreateCommand<getRentalById>(ds)
    use cmd1 = cmd ()
    Assert.Equal(1, cmd1.TaskAsyncExecute( rental_id).Result |> Seq.length) 

    do 
        use conn = openConnection()
        use tran = conn.BeginTransaction()

        use del = 
            DvdRental.CreateCommand<"
                DELETE FROM rental WHERE rental_id = @rental_id
            ", XCtor = true>(conn, tran)  
        Assert.Equal(1, del.TaskAsyncExecute(rental_id).Result)
        Assert.Empty( DvdRental.CreateCommand<getRentalById, XCtor = true>(conn, tran).TaskAsyncExecute( rental_id).Result) 


    use cmd2 = cmd ()
    Assert.Equal(1, cmd2.TaskAsyncExecute( rental_id).Result |> Seq.length) 
    
[<Fact>]
let ``Select from partitioned table``() =
    use cmd = DvdRental.CreateCommand<selectFromPartitionedTable>(ds)
    let actual = cmd.TaskAsyncExecute().Result
    Assert.Equal(2, actual.Length)
    Assert.Equal<int[]>([|1;2;3|], actual.Head.some_data)

[<Fact>]
let ``Select from specific partition``() =
    use cmd = DvdRental.CreateCommand<selectFromSpecificPartition>(ds)
    let actual = cmd.TaskAsyncExecute().Result
    Assert.Equal(2, actual.Length)
    Assert.Equal<int[]>([|1;2;3|], actual.Head.some_data)

type Rating = DvdRental.``public``.Types.mpaa_rating


[<Fact>]
let selectEnum() =
    
    use cmd = 
        DvdRental.CreateCommand<"
            SELECT * 
            FROM UNNEST( enum_range(NULL::mpaa_rating)) AS X 
            WHERE X <> @exclude;          
        ">(ds)
    Assert.Equal<_ list>(
        [ Rating.G; Rating.PG; Rating.R; Rating.``NC-17`` ],
        [ for x in cmd.TaskAsyncExecute(exclude = Rating.``PG-13``).Result -> x.Value ]
    ) 

[<Fact>]
let selectEnumWithArray() =
    use cmd = DvdRental.CreateCommand<"
        SELECT COUNT(*)  FROM film WHERE ARRAY[rating] <@ @xs::text[]::mpaa_rating[];
    ", SingleRow = true>(ds)

    Assert.Equal( Some( Some 223L), cmd.TaskAsyncExecute([| "PG-13" |]).Result) 

[<Fact>]
let allParametersOptional() =
    let cmd () = 
        DvdRental.CreateCommand<"
            SELECT coalesce(@x, 'Empty') AS x
        ", AllParametersOptional = true, SingleRow = true>(ds)
    Assert.Equal(Some( Some "test"), cmd().TaskAsyncExecute(Some "test").Result) 
    Assert.Equal(Some( Some "Empty"), cmd().TaskAsyncExecute().Result) 

[<Fact>]
let selectSingleRowSingleColumnNonNullable() =
    use cmd = DvdRental.CreateCommand<"select film_id from film where film_id = 1", SingleRow = true>(ds)
    Assert.Equal(Some 1, cmd.TaskAsyncExecute().Result) 

[<Fact>]
let tableInsert() =
    
    let rental_id = 2
    
    let cmd () = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true>(ds)  
    use cmd1 = cmd ()
    let x = cmd1.TaskAsyncExecute(rental_id).Result |> Option.get
        
    use conn = openConnection()
    use tran = conn.BeginTransaction()
    use t = new DvdRental.``public``.Tables.rental()
    let r = 
        t.NewRow(
            staff_id = x.staff_id, 
            customer_id = x.customer_id, 
            inventory_id = x.inventory_id, 
            rental_date = x.rental_date.AddDays(1.), 
            return_date = x.return_date
        )

    t.Rows.Add(r)
    Assert.Equal(1, t.Update(conn, tran))
    let y = 
        use cmd = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true, XCtor = true>(conn, tran)
        cmd.TaskAsyncExecute(r.rental_id).Result |> Option.get

    Assert.Equal(x.staff_id, y.staff_id)
    Assert.Equal(x.customer_id, y.customer_id)
    Assert.Equal(x.inventory_id, y.inventory_id)
    Assert.Equal(x.rental_date.AddDays(1.), y.rental_date)
    Assert.Equal(x.return_date, y.return_date)

    tran.Rollback()

    use cmd2 = cmd ()
    Assert.Equal(None, cmd2.TaskAsyncExecute(r.rental_id).Result)

[<Fact>]
let tableInsertViaAddRow() =
    
    let rental_id = 2
    
    let cmd () = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true>(ds)
    use cmd1 = cmd ()
    let x = cmd1.TaskAsyncExecute(rental_id).Result |> Option.get
        
    use conn = openConnection()
    use tran = conn.BeginTransaction()
    use t = new DvdRental.``public``.Tables.rental()

    t.AddRow(
        staff_id = x.staff_id, 
        customer_id = x.customer_id, 
        inventory_id = x.inventory_id, 
        rental_date = x.rental_date.AddDays(1.), 
        return_date = x.return_date
    )

    let r = t.Rows.[t.Rows.Count - 1]

    Assert.Equal(1, t.Update(connection = conn, transaction = tran))
    let y = 
        use cmd = DvdRental.CreateCommand<"SELECT * FROM rental WHERE rental_id = @rental_id", SingleRow = true, XCtor = true>(conn, tran)
        cmd.TaskAsyncExecute(r.rental_id).Result |> Option.get

    Assert.Equal(x.staff_id, y.staff_id)
    Assert.Equal(x.customer_id, y.customer_id)
    Assert.Equal(x.inventory_id, y.inventory_id)
    Assert.Equal(x.rental_date.AddDays(1.), y.rental_date)
    Assert.Equal(x.return_date, y.return_date)

    tran.Rollback()

    use cmd2 = cmd ()
    Assert.Equal(None, cmd2.TaskAsyncExecute(r.rental_id).Result)
  
[<Fact>]
let selectEnumWithArray2() =
    use cmd = DvdRental.CreateCommand<"SELECT @ratings::mpaa_rating[];", SingleRow = true>(ds)

    let ratings = [| 
        DvdRental.``public``.Types.mpaa_rating.``PG-13``
        DvdRental.``public``.Types.mpaa_rating.R
    |]
        
    Assert.Equal( Some(  Some ratings), cmd.TaskAsyncExecute(ratings).Result)

[<Fact>]
let selectLiteralsWithConnObject() =
    use cmd = 
        DvdRental.CreateCommand<"SELECT 42 AS Answer, current_date as today", XCtor = true>(openConnection ())

    let x = cmd.TaskAsyncExecute().Result |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer) 
    Assert.Equal(Some DateTime.Now.Date, x.today)


type DvdRentalWithConn = NpgsqlConnection<connectionString, XCtor = true>

[<Fact>]
let selectLiteralsWithConnObjectGlobalSet() =
    use cmd = 
        DvdRentalWithConn.CreateCommand<"SELECT 42 AS Answer, current_date as today">(openConnection ())

    let x = cmd.TaskAsyncExecute().Result |> Seq.exactlyOne
    Assert.Equal(Some 42, x.answer) 
    Assert.Equal(Some DateTime.Now.Date, x.today)

[<Fact>]
let ``AddRow/NewRow preserve order``() =
    let actors = new DvdRental.``public``.Tables.actor()
    let r = actors.NewRow(Some 42, "Tom", "Hanks", Some DateTime.Now)
    actors.Rows.Add(r); actors.Rows.Remove(r) |> ignore
    let r = actors.NewRow(actor_id = Some 42, first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now)
    actors.Rows.Add(r); actors.Rows.Remove(r) |> ignore
    
    actors.AddRow(first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now)
    actors.AddRow(last_update = Some DateTime.Now, first_name = "Tom", last_name = "Hanks")

    let films = new DvdRental.``public``.Tables.film()
    films.AddRow(
        title = "Inception", 
        description = Some "A thief, who steals corporate secrets through the use of dream-sharing technology, is given the inverse task of planting an idea into the mind of a CEO.",
        language_id = 1s,
        rating = Some Rating.``PG-13``,
        fulltext = NpgsqlTypes.NpgsqlTsVector.Parse("")
    )
    use conn = openConnection()
    use tx = conn.BeginTransaction()
    Assert.Equal(1, films.Update(conn, tx))

[<Fact>]
let Add2Rows() =
    use conn = openConnection()
    use tx = conn.BeginTransaction()
    let actors = new DvdRental.``public``.Tables.actor()
    actors.AddRow(first_name = "Tom", last_name = "Hanks")
    actors.AddRow(first_name = "Tom", last_name ="Cruise", last_update = Some DateTime.Now)
    let i = actors.Update(conn, tx)
    Assert.Equal(actors.Rows.Count, i)

[<Fact>]
let asyncUpdateTable() =

    use conn = openConnection()
    use tx = conn.BeginTransaction()
    use cmd =
        DvdRental.CreateCommand<"
            SELECT 
                actor_id, first_name, last_name, last_update
            FROM 
                public.actor
            WHERE 
                first_name = @firstName 
                AND last_name = @firstNameNotReally
        ", ResultType.DataTable, XCtor = true>(conn, tx)
    let (firstName, lastName) as name = "Tom", "Hanks"
    let actors = cmd.TaskAsyncExecute(name).Result

    if actors.Rows.Count = 0 then
        actors.AddRow(first_name = firstName, last_name = lastName)
    else
        actors.Rows.[0].last_update <- DateTime.UtcNow
    
    Assert.Equal(1, actors.Update(conn, tx))

[<Fact>]
let binaryImport() =
    let firstName, lastName = "Tom", "Hanks"
    do 
        use conn = openConnection()
        use tx = conn.BeginTransaction()
        let actors = new DvdRental.``public``.Tables.actor()
        
        let actor_id = 
            use cmd = DvdRental.CreateCommand<"select nextval('actor_actor_id_seq' :: regclass)::int", SingleRow = true, XCtor = true>(conn, tx)
            cmd.TaskAsyncExecute().Result |> Option.flatten 
        
        actors.AddRow(actor_id, first_name = "Tom", last_name = "Hanks", last_update = Some DateTime.Now)
        let importedCount = actors.BinaryImport(conn, false, CancellationToken.None).Result
        Assert.Equal(1UL, importedCount)

        use cmd = DvdRental.CreateCommand<getActorByName, XCtor = true>(conn, tx)
        Assert.Equal(1, cmd.TaskAsyncExecute(firstName, lastName).Result |> Seq.length)
    do 
        use cmd = DvdRental.CreateCommand<getActorByName>(ds)
        Assert.Equal(0, cmd.TaskAsyncExecute(firstName, lastName).Result |> Seq.length)

[<Fact>]
let ``binaryImport ignores identity columns when set`` () =
    use conn = openConnection ()
    use tran = conn.BeginTransaction ()

    let table = new DvdRental.``public``.Tables.table_with_identity ()
    table.AddRow (stuff = "one")
    table.AddRow (stuff = "two")

    Assert.Equal (2UL, table.BinaryImport(conn, true, CancellationToken.None).Result)
    tran.Rollback ()

[<Fact>]
let ``binaryImport does not ignore identity columns when not set`` () =
    use conn = openConnection ()
    use tran = conn.BeginTransaction ()

    let table = new DvdRental.``public``.Tables.table_with_identity ()
    table.AddRow (stuff = "one")
    table.AddRow (stuff = "two")

    let e = Assert.Throws<AggregateException>(fun () -> table.BinaryImport(conn, false, CancellationToken.None).Wait ()).InnerException :?> Npgsql.PostgresException
    Assert.Equal ("23505", e.SqlState) // primary key violation
    tran.Rollback ()
    use tran = conn.BeginTransaction ()

    table.Rows.Clear ()
    table.AddRow (Some 1000, stuff = "one")
    table.AddRow (Some 1001, stuff = "two")
    Assert.Equal (2UL, table.BinaryImport(conn, false, CancellationToken.None).Result)

    use cmd = DvdRental.CreateCommand<"select * from table_with_identity", XCtor = true>(conn)
    let data = cmd.TaskAsyncExecute().Result
    Assert.Equal ("one", data |> List.pick (fun x -> if x.id = 1000 then Some x.stuff else None))
    Assert.Equal ("two", data |> List.pick (fun x -> if x.id = 1001 then Some x.stuff else None))

    tran.Rollback ()

[<Fact>]
let batchSize() =
    use conn = openConnection()
    use tx = conn.BeginTransaction()
    let actors = new DvdRental.``public``.Tables.actor()
    actors.AddRow(first_name = "Tom", last_name = "Hanks")
    actors.AddRow(first_name = "Tom", last_name ="Cruise", last_update = Some DateTime.Now)
    let i = actors.Update(conn, tx, batchSize = 100)
    Assert.Equal(actors.Rows.Count, i)

    for row in actors.Rows do 
        use cmd = DvdRental.CreateCommand<getActorByName, SingleRow = true, XCtor = true>(conn, tx)
        let res = cmd.TaskAsyncExecute(row.first_name, row.last_name).Result |> Option.map (fun x -> x.first_name, x.last_name)
        Assert.Equal(Some(row.first_name, row.last_name), res)

[<Fact>]
let batchSizeConflictOptionCompareAllSearchableValue() =
    use conn = openConnection()
    use tx = conn.BeginTransaction()
    let actors = new DvdRental.``public``.Tables.actor()
    actors.AddRow(first_name = "Tom", last_name = "Hanks")
    actors.AddRow(first_name = "Tom", last_name ="Cruise", last_update = Some DateTime.Now)
    let i = actors.Update(conn, tx, batchSize = 100, conflictOption = Data.ConflictOption.CompareAllSearchableValues)
    Assert.Equal(actors.Rows.Count, i)

    for row in actors.Rows do 
        use cmd = DvdRental.CreateCommand<getActorByName, SingleRow = true, XCtor = true>(conn, tx)
        let res = cmd.TaskAsyncExecute(row.first_name, row.last_name).Result |> Option.map (fun x -> x.first_name, x.last_name)
        Assert.Equal(Some(row.first_name, row.last_name), res)

[<Fact>]
let ``column "p1_00" does not exist``() =
    use conn = openConnection()
    use tx = conn.BeginTransaction()
    let nextFildId = 
        use cmd =  DvdRental.CreateCommand<"select nextval('film_film_id_seq' :: regclass)", SingleRow = true, XCtor = true>(conn, tx)
        cmd.TaskAsyncExecute().Result |> Option.flatten |> Option.map int

    let expected = [ for i in 0..100 -> Option.map ((+) i) nextFildId, sprintf "title %i" i]
    
    let films = new DvdRental.``public``.Tables.film()
    for id, title in expected do 
        films.AddRow(
            film_id = id, 
            title = title, 
            description = Some "Some description", 
            release_year = None, 
            language_id = 1s, 
            rental_duration = Some 6s, 
            rental_rate = Some 0.9M, 
            length = Some 100s, 
            replacement_cost = Some 12M, 
            rating = Some Rating.PG, 
            last_update = Some DateTime.Now, 
            special_features = Some [| "Deleted Scenes" |] , 
            fulltext = NpgsqlTypes.NpgsqlTsVector.Parse("")
        )

    let i = films.Update(conn, tx, batchSize = 10)
    Assert.Equal(films.Rows.Count, i)

    for id, title in expected do 
        use cmd = DvdRental.CreateCommand<"select title from public.film where film_id = @id", SingleRow = true, XCtor = true>(conn, tx)
        Assert.Equal( Some title, cmd.TaskAsyncExecute(id.Value).Result)

[<Fact>]
let selectBytea() =
    use cmd = DvdRental.CreateCommand<"SELECT picture FROM public.staff WHERE staff_id = 1", SingleRow = true>(ds)
    let actual = cmd.TaskAsyncExecute().Result.Value.Value
    let expected = [|137uy; 80uy; 78uy; 71uy; 13uy; 10uy; 90uy; 10uy|]
    Assert.Equal<byte>(expected, actual)

[<Fact>]
let ``Select from materialized view``() =
    use cmd = DvdRental.CreateCommand<"select some_data, title from long_films limit 1", SingleRow = true>(ds)
    let actual = cmd.TaskAsyncExecute().Result.Value
    Assert.Equal<int[]>([|1;2;3|], actual.some_data.Value)
    Assert.True(String.IsNullOrWhiteSpace actual.title.Value |> not)

[<Fact>]
let ``Simple nonquery works`` () =
    use conn = openConnection ()
    use tran = conn.BeginTransaction ()
    use cmd = DvdRental.CreateCommand<"delete from logs", XCtor = true>(conn, tran)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.True (actual > 0)
    tran.Rollback ()

[<Fact>]
let ``Command not prepared by default``() =
    use conn = openConnection()
    conn.UnprepareAll()

    use cmd = DvdRental.CreateCommand<getActorByName, XCtor = true>(conn)
    cmd.TaskAsyncExecute("", "").Wait ()

    Assert.False(isStatementPrepared conn)

[<Fact>]
let ``Data table command prepared``() =
    use conn = openConnection()
    conn.UnprepareAll()

    use cmd = DvdRental.CreateCommand<getActorByName, XCtor = true, Prepare = true, ResultType = ResultType.DataTable>(conn)
    cmd.TaskAsyncExecute("", "").Wait ()

    Assert.True(isStatementPrepared conn)

[<Fact>]
let ``Data reader command prepared``() =
    use conn = openConnection()
    conn.UnprepareAll()

    use cmd = DvdRental.CreateCommand<getActorByName, XCtor = true, Prepare = true, ResultType = ResultType.DataReader>(conn)
    cmd.TaskAsyncExecute("", "").Wait ()

    Assert.True(isStatementPrepared conn)

[<Fact>]
let ``Records command prepared``() =
    use conn = openConnection()
    conn.UnprepareAll()

    use cmd = DvdRental.CreateCommand<getActorByName, XCtor = true, Prepare = true, ResultType = ResultType.Records>(conn)
    cmd.TaskAsyncExecute("1", "2").Wait ()

    Assert.True(isStatementPrepared conn)

[<Fact>]
let ``Tuples command prepared``() =
    use conn = openConnection()
    conn.UnprepareAll()

    use cmd = DvdRental.CreateCommand<getActorByName, XCtor = true, Prepare = true, ResultType = ResultType.Tuples>(conn)
    cmd.TaskAsyncExecute("", "").Wait ()

    Assert.True(isStatementPrepared conn)

[<Fact>]
let ``Two selects record``() =
    use cmd = DvdRental.CreateCommand<getActorsAndFilms>(ds)
    let actual = cmd.TaskAsyncExecute().Result
    Assert.Equal (5, actual.ResultSet1 |> List.map (fun x -> x.first_name) |> List.length)
    Assert.Equal (5, actual.ResultSet2 |> List.map (fun x -> x.title) |> List.length)
    
[<Fact>]
let ``Two selects record2``() =
    use cmd = DvdRental.CreateCommand<"select actor_id as id, first_name as name, @arr::integer[] from actor where actor_id % @mod = 0 and @f::boolean order by id limit 5; select title as name, film_id as id, @arr::integer[] from film where @f::boolean and film_id % @mod = 0 order by id limit 5">(ds)
    let arr = [| 9; 6 |]
    let actual = cmd.TaskAsyncExecute(arr, 2, true).Result
    Assert.Equal ("Nick", actual.ResultSet1.[0].name)
    Assert.Equal ("Ace Goldfinger", actual.ResultSet2.[0].name)
    Assert.Equal<int> (arr, actual.ResultSet1.[0].int4.Value)
    Assert.Equal<int> (arr, actual.ResultSet2.[0].int4.Value)

[<Fact>]
let ``Two selects record3``() =
    use cmd = DvdRental.CreateCommand<"select actor_id as id, first_name as name, @arr::integer[] from actor where actor_id % @mod = 0 and @f::boolean order by id limit 5; select title as name, film_id as id, @arr::integer[] from film where @f::boolean and film_id % @mod = 0 order by id limit 5">(ds)
    let arr = [| 9; 6 |]
    let actual = cmd.TaskAsyncExecute(arr, 2, true).Result
    Assert.Equal ("Nick", actual.ResultSet1.[0].name)
    Assert.Equal ("Ace Goldfinger", actual.ResultSet2.[0].name)
    Assert.Equal<int> (arr, actual.ResultSet1.[0].int4.Value)
    Assert.Equal<int> (arr, actual.ResultSet2.[0].int4.Value)

[<Fact>]
let ``Queries against system catalogs work``() =
    use cmd = DvdRental.CreateCommand<"SELECT * FROM pg_timezone_names">(ds)
    let actual = cmd.TaskAsyncExecute().Result
    Assert.True(actual |> List.map (fun x -> x.name.Value) |> List.length > 0)    

[<Fact>]
let ``Two selects tuple``() =
    use cmd = DvdRental.CreateCommand<getActorsAndFilms, ResultType = ResultType.Tuples>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.ResultSet1 |> List.length)
    Assert.Equal (5, actual.ResultSet2 |> List.length)

[<Fact>]
let ``Two selects data table``() =
    use cmd = DvdRental.CreateCommand<getActorsAndFilms, ResultType = ResultType.DataTable>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.ResultSet1.Rows |> Seq.map (fun x -> x.first_name) |> Seq.length)
    Assert.Equal (5, actual.ResultSet2.Rows |> Seq.map (fun x -> x.title) |> Seq.length)

[<Fact>]
let ``Two selects data reader``() =
    use cmd = DvdRental.CreateCommand<getActorsAndFilms, ResultType = ResultType.DataReader>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    let mutable resultSets = 1

    while actual.NextResult () do
        resultSets <- resultSets + 1

    Assert.Equal (2, resultSets)

[<Fact>]
let ``Two selects and nonquery record``() =
    use cmd = DvdRental.CreateCommand<getActorsUpdateActorsGetFilms>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.ResultSet1 |> List.map (fun x -> x.first_name) |> List.length)
    Assert.Equal (1, actual.RowsAffected2)
    Assert.Equal (5, actual.ResultSet3 |> List.map (fun x -> x.title) |> List.length)

[<Fact>]
let ``Two selects and nonquery tuple``() =
    use cmd = DvdRental.CreateCommand<getActorsUpdateActorsGetFilms, ResultType = ResultType.Tuples>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.ResultSet1 |> List.length)
    Assert.Equal (1, actual.RowsAffected2)
    Assert.Equal (5, actual.ResultSet3 |> List.length)

[<Fact>]
let ``Two selects and nonquery data table``() =
    use cmd = DvdRental.CreateCommand<getActorsUpdateActorsGetFilms, ResultType = ResultType.DataTable>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.ResultSet1.Rows |> Seq.map (fun x -> x.first_name) |> Seq.length)
    Assert.Equal (1, actual.RowsAffected2)
    Assert.Equal (5, actual.ResultSet3.Rows |> Seq.map (fun x -> x.title) |> Seq.length)

[<Fact>]
let ``Two selects and nonquery data reader``() =
    use cmd = DvdRental.CreateCommand<getActorsUpdateActorsGetFilms, ResultType = ResultType.DataReader>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    let mutable resultSets = 1

    while actual.NextResult () do
        resultSets <- resultSets + 1

    Assert.Equal (2, resultSets)

[<Fact>]
let ``Four single-column selects``() =
    use cmd = DvdRental.CreateCommand<fourSelects>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (10, actual.ResultSet1 |> Seq.map (fun x -> x.Value) |> Seq.length)
    Assert.Equal (10, actual.ResultSet2 |> Seq.map (fun x -> x.Value) |> Seq.length)
    Assert.Equal (10, actual.ResultSet3 |> Seq.map (fun x -> x.Value) |> Seq.length)
    Assert.Equal (10, actual.ResultSet4 |> Seq.map (fun x -> x.Value) |> Seq.length)

[<Fact>]
let ``One select and two updates record``() =
    use cmd = DvdRental.CreateCommand<updateActorsUpdateSelectActorsUpdateFilms>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (2, actual.RowsAffected1)
    Assert.Equal (5, actual.ResultSet2 |> List.map (fun x -> x.first_name) |> List.length)
    Assert.Equal (1, actual.RowsAffected3)

[<Fact>]
let ``One select and two updates tuple``() =
    use cmd = DvdRental.CreateCommand<updateActorsUpdateSelectActorsUpdateFilms, ResultType = ResultType.Tuples>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (2, actual.RowsAffected1)
    Assert.Equal (5, actual.ResultSet2 |> List.length)
    Assert.Equal (1, actual.RowsAffected3)

[<Fact>]
let ``One select and two updates data table``() =
    use cmd = DvdRental.CreateCommand<updateActorsUpdateSelectActorsUpdateFilms, ResultType = ResultType.DataTable>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (2, actual.RowsAffected1)
    Assert.Equal (5, actual.ResultSet2.Rows |> Seq.map (fun x -> x.first_name) |> Seq.length)
    Assert.Equal (1, actual.RowsAffected3)

[<Fact>]
let ``One select and two updates reader``() =
    use cmd = DvdRental.CreateCommand<updateActorsUpdateSelectActorsUpdateFilms, ResultType = ResultType.DataReader>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    let mutable resultSets = 1

    while actual.NextResult () do
        resultSets <- resultSets + 1

    Assert.Equal (1, resultSets)

[<Fact>]
let ``Begin/end are ignored and don't generate a result set``() =
    use cmd = DvdRental.CreateCommand<"begin   ; delete from film where film_id = -200;select * from film where film_id = -200;END">(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (0, actual.RowsAffected2)
    Assert.Equal (0, actual.ResultSet3.Length)

// Necessary to be able to refer to the reused type in the function below
let _ = DvdRental.CreateCommand<"select film_id, rating from film", SingleRow = true>

type FilmIdRating = DvdRental.``film_id:Int32, rating:Option<public.mpaa_rating>``

let assertEqualFilmIdRating (x: FilmIdRating) (y: FilmIdRating) =
    Assert.Equal (x.film_id, y.film_id)
    Assert.Equal (x.rating, y.rating)

[<Fact>]
let ``Record type reused regardless of column order``() =
    use cmd1 = DvdRental.CreateCommand<"select film_id, rating from film limit 1", SingleRow = true>(ds)
    use cmd2 = DvdRental.CreateCommand<"select rating, film_id from film limit 1", SingleRow = true>(ds)
    let actual1 = cmd1.TaskAsyncExecute().Result.Value
    let actual2 = cmd2.TaskAsyncExecute().Result.Value

    assertEqualFilmIdRating actual1 actual2

[<Fact>]
let ``Record type reused within a single command with multiple statements``() =
    use cmd = DvdRental.CreateCommand<"select rating, film_id from film limit 1; select film_id, rating from film limit 1">(ds)
    let res = cmd.TaskAsyncExecute().Result
    let actual1 = res.ResultSet1.Head
    let actual2 = res.ResultSet2.Head

    assertEqualFilmIdRating actual1 actual2

[<Fact>]
let ``Bytea is properly encoded in reused type name``() =
    use cmd = DvdRental.CreateCommand<"SELECT staff_id, picture FROM public.staff WHERE staff_id = 1", SingleRow = true>(ds)
    let actual = cmd.TaskAsyncExecute().Result.Value
    let expected = [|137uy; 80uy; 78uy; 71uy; 13uy; 10uy; 90uy; 10uy|]

    let assertByteaEqual (actual: DvdRental.``picture:Option<Byte[]>, staff_id:Int32``) =
        Assert.Equal<byte> (expected, actual.picture.Value)

    assertByteaEqual actual

[<Fact>]
let ``Record rows contain different values``() =
    use cmd = DvdRental.CreateCommand<"SELECT staff_id, picture FROM public.staff">(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.NotEqual (actual.[0].staff_id, actual.[1].staff_id)

[<Fact>]
let ``Interval update works``() =
    let entryId = Guid.NewGuid()
    use insertCommand = DvdRental.CreateCommand<"INSERT INTO public.logs (id, log_time, some_data, modified) VALUES (@id, '2022-2-2', '{2}','22 hours')">(ds)
    insertCommand.TaskAsyncExecute(entryId).Wait ()


    // Now select for update
    use cmdLog = DvdRental.CreateCommand<"SELECT * FROM public.logs WHERE id = @entry_id", ResultType.DataTable>(ds)
    let tblLog = cmdLog.TaskAsyncExecute(entry_id = entryId).Result
    let logRow = tblLog.Rows.[0]

    // Now change it to 33 hours
    let newTimespan = TimeSpan.FromHours 33.

    logRow.modified <- newTimespan

    let updatedRows = tblLog.Update(ds)
    Assert.Equal(1,updatedRows)
    
    // Now check one last time what row 2 is
    use cmd = DvdRental.CreateCommand<"SELECT id, modified FROM public.logs WHERE id = @entry_id",SingleRow=true>(ds)
    let row = cmd.TaskAsyncExecute(entry_id = entryId).Result
    let expectedTS = TimeSpan(1,9,0,0) // 33 hours
    Assert.Equal(expectedTS, row.Value.modified)

    use cleanupCommand = DvdRental.CreateCommand<"DELETE FROM public.logs WHERE id = @id">(ds)
    cleanupCommand.TaskAsyncExecute(entryId).Wait ()

[<Fact>]
let ``Insert does skip computed columns``() =
    use table = new DvdRental.``public``.Tables.table_with_computed_columns()
    let row = table.NewRow(operand_1 = 10, operand_2 = 20)
    table.Rows.Add(row)
    table.Update(ds) |> ignore

[<Fact>]
let ``Array collection type with records works`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film limit 5", CollectionType = CollectionType.Array>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, Array.length actual)

[<Fact>]
let ``Array collection type with records and multiple result sets works`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film limit 5; select * from actor limit 6", CollectionType = CollectionType.Array>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, Array.length actual.ResultSet1)
    Assert.Equal (6, Array.length actual.ResultSet2)

[<Fact>]
let ``Array collection type with tuples works`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film limit 5", CollectionType = CollectionType.Array, ResultType = ResultType.Tuples>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, Array.length actual)

[<Fact>]
let ``ResizeArray collection type with records works`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film limit 5", CollectionType = CollectionType.ResizeArray>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.Count)

[<Fact>]
let ``ResizeArray collection type with tuples works`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film limit 5", CollectionType = CollectionType.ResizeArray, ResultType = ResultType.Tuples>(ds)
    let actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.Count)

[<Fact>]
let ``LazySeq works`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film", CollectionType = CollectionType.LazySeq>(ds)
    use actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.Seq |> Seq.take 5 |> Seq.length)

    use cmd = DvdRental.CreateCommand<"SELECT film_id, rating from film", CollectionType = CollectionType.LazySeq>(ds)
    use actual = cmd.TaskAsyncExecute().Result

    Assert.Equal (5, actual.Seq |> Seq.take 5 |> Seq.length)

    use cmd = DvdRental.CreateCommand<"SELECT null::integer blah from film", CollectionType = CollectionType.LazySeq>(ds)
    use actual = cmd.TaskAsyncExecute().Result

    actual.Seq
    |> Seq.take 5
    |> Seq.iter (fun v -> Assert.Equal (None, v))

[<Fact>]
let ``Disposing of the command does not dispose of the underlying Npgsql objects and LazySeq still works`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film", CollectionType = CollectionType.LazySeq>(ds)
    use actual = cmd.TaskAsyncExecute().Result
    (cmd :> IDisposable).Dispose ()

    Assert.Equal (5, actual.Seq |> Seq.take 5 |> Seq.length)

[<Fact>]
let ``Disposing of the LazySeq causes further enumerations to fail`` () =
    use cmd = DvdRental.CreateCommand<"SELECT * from film", CollectionType = CollectionType.LazySeq>(ds)
    use actual = cmd.TaskAsyncExecute().Result
    (actual :> IDisposable).Dispose ()

    Assert.Throws<ObjectDisposedException> (fun () -> actual.Seq |> Seq.take 5 |> Seq.length |> ignore) |> ignore

[<Fact>]
let ``Disposing of the LazySeq disposes of the reader, but the provided connection stays open`` () =
    use conn = openConnection()
    use cmd = DvdRental.CreateCommand<"SELECT * from film", CollectionType = CollectionType.LazySeq, XCtor = true>(conn)
    use actual = cmd.TaskAsyncExecute().Result
    (actual :> IDisposable).Dispose ()

    Assert.Throws<ObjectDisposedException> (fun () -> actual.Seq |> Seq.take 5 |> Seq.length |> ignore) |> ignore
    Assert.Equal (System.Data.ConnectionState.Open, conn.State)

[<Fact>]
let ``Disposing of the command does not close the connection in case of xctor`` () =
    use conn = openConnection()
    use cmd = DvdRental.CreateCommand<"SELECT * from film limit 1", XCtor = true>(conn)
    let _ = cmd.TaskAsyncExecute().Result
    (cmd :> IDisposable).Dispose ()

    Assert.Equal (System.Data.ConnectionState.Open, conn.State)

[<Fact>]
let ``Manually mapped and cast composite type works`` () =
    use cmd = DvdRental.CreateCommand<"select simple from table_with_composites where id = 1", SingleRow = true>(ds)
    let res = cmd.TaskAsyncExecute().Result.Value :?> SimpleComposite

    Assert.Equal (42L, res.SomeNumber)
    Assert.Equal ("blah", res.SomeText)
    Assert.Equal<int> ([| 1; 2 |], res.SomeArray)

[<Fact>]
let ``NetTopology.Geometry roundtrip works`` () =
    let input = Geometry.DefaultFactory.CreatePoint (Coordinate (55., 0.))
    use cmd = DvdRental.CreateCommand<"select @p::geometry">(ds)
    let res = cmd.TaskAsyncExecute(input).Result.Head.Value
    
    Assert.Equal (input.Coordinate.X, res.Coordinate.X)

[<Fact>]
let ``NetTopology.Geometry roundtrip works record`` () =
    let input = Geometry.DefaultFactory.CreatePoint (Coordinate (55., 0.))
    use cmd = DvdRental.CreateCommand<"select @p::geometry g, 0 blah, null::geometry gg">(ds)
    let res = cmd.TaskAsyncExecute(input).Result.Head.g.Value
    
    Assert.Equal (input.Coordinate.X, res.Coordinate.X)

[<Fact>]
let ``NetTopology.Geometry roundtrip works record single row`` () =
    let input = Geometry.DefaultFactory.CreatePoint (Coordinate (55., 0.))
    use cmd = DvdRental.CreateCommand<"select @p::geometry g, 0 blah, null::geometry gg", SingleRow = true>(ds)
    let res = cmd.TaskAsyncExecute(input).Result.Value
    
    Assert.Equal (input.Coordinate.X, res.g.Value.Coordinate.X)
    Assert.Equal (None, res.gg)

[<Fact>]
let ``NetTopology.Geometry roundtrip works tuple`` () =
    let input = Geometry.DefaultFactory.CreatePoint (Coordinate (55., 0.))
    use cmd = DvdRental.CreateCommand<"select @p::geometry g, 0 blah, null::geometry gg", ResultType = ResultType.Tuples>(ds)
    let res, _, _ = cmd.TaskAsyncExecute(input).Result.Head
    
    Assert.Equal (input.Coordinate.X, res.Value.Coordinate.X)
