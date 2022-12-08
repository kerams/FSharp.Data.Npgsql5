namespace FSharp.Data.Npgsql

open System
open System.Data
open Npgsql
open System.ComponentModel
open System.Reflection
open System.Collections.Concurrent
open FSharp.Control.Tasks.NonAffine
open type Utils

type internal ExecutionType =
    | Sync
    | Async
    | TaskAsync

[<EditorBrowsable(EditorBrowsableState.Never)>]
type ISqlCommand = 
    abstract Execute: unit -> obj
    abstract AsyncExecute: unit -> obj
    abstract TaskAsyncExecute: unit -> obj

[<EditorBrowsable(EditorBrowsableState.Never); NoEquality; NoComparison>]
type DesignTimeConfig = {
    ResultType: ResultType
    CollectionType: CollectionType
    SingleRow: bool
    ResultSets: ResultSetDefinition[]
    Prepare: bool
}
    with
        static member Create (resultType, collection, singleRow, (columns: DataColumn[][]), prepare) = {
            ResultType = resultType
            CollectionType = collection
            SingleRow = singleRow
            ResultSets = columns |> Array.map (fun r -> CreateResultSetDefinition (r, resultType))
            Prepare = prepare }

[<EditorBrowsable(EditorBrowsableState.Never)>]
type ISqlCommandImplementation (commandNameHash: int, cfgBuilder: unit -> DesignTimeConfig, connection, cmd) =
    static let cfgExecuteCache = ConcurrentDictionary ()
    static let executeSingleCache = ConcurrentDictionary ()

    let cfg, execute =
        let mutable x = Unchecked.defaultof<_>
        if cfgExecuteCache.TryGetValue (commandNameHash, &x) then
            x
        else
            let cfg = cfgBuilder ()

            let execute =
                match cfg.ResultType with
                | ResultType.DataReader ->
                    ISqlCommandImplementation.AsyncExecuteReader
                | ResultType.DataTable ->
                    if cfg.ResultSets.Length = 1 then
                        ISqlCommandImplementation.AsyncExecuteDataTable
                    else
                        ISqlCommandImplementation.AsyncExecuteDataTables
                | ResultType.Records | ResultType.Tuples ->
                    match cfg.ResultSets with
                    | [| resultSet |] ->
                        if resultSet.ExpectedColumns.Length = 0 then
                            ISqlCommandImplementation.AsyncExecuteNonQuery
                        else
                            typeof<ISqlCommandImplementation>
                                .GetMethod(nameof ISqlCommandImplementation.AsyncExecuteList, BindingFlags.NonPublic ||| BindingFlags.Static)
                                .MakeGenericMethod(resultSet.ErasedRowType)
                                .Invoke(null, [||]) |> unbox
                    | _ ->
                        ISqlCommandImplementation.AsyncExecuteMulti
                | unexpected -> failwithf "Unexpected ResultType value: %O" unexpected

            cfgExecuteCache.[commandNameHash] <- (cfg, execute)
            cfg, execute

    static let getReaderBehavior (connection, cfg) = 
        // Don't pass CommandBehavior.SingleRow to Npgsql, because it only applies to the first row of the first result set and all other result sets are completely ignored
        if cfg.SingleRow && cfg.ResultSets.Length = 1 then CommandBehavior.SingleRow else CommandBehavior.Default
        ||| if cfg.ResultType = ResultType.DataTable then CommandBehavior.KeyInfo else CommandBehavior.Default
        ||| match connection with Choice1Of2 _ -> CommandBehavior.CloseConnection | _ -> CommandBehavior.Default

    static let setupConnection (cmd: NpgsqlCommand, connection) =
        match connection with
        | Choice2Of2 (conn, tx) ->
            cmd.Connection <- conn
            cmd.Transaction <- tx
            System.Threading.Tasks.Task.CompletedTask
        | Choice1Of2 connectionString ->
            cmd.Connection <- new NpgsqlConnection (connectionString)
            cmd.Connection.OpenAsync ()

    static let mapTask (t: Ply.Ply<_>, executionType) =
        let t = task { return! t }

        match executionType with
        | Sync -> box t.Result
        | Async -> Async.AwaitTask t |> box
        | TaskAsync -> box t

    member val NpgsqlCommand: NpgsqlCommand = cmd

    interface ISqlCommand with
        member x.Execute () = execute (cfg, x.NpgsqlCommand, connection, Sync)
        member x.AsyncExecute () = execute (cfg, x.NpgsqlCommand, connection, Async)
        member x.TaskAsyncExecute () = execute (cfg, x.NpgsqlCommand, connection, TaskAsync)

    interface IDisposable with
        member x.Dispose () =
            if cfg.CollectionType <> CollectionType.LazySeq then
                x.NpgsqlCommand.Dispose ()

    static member internal VerifyOutputColumns(cursor: Common.DbDataReader, expectedColumns: DataColumn[]) =
        if cursor.FieldCount < expectedColumns.Length then
            let message = sprintf "Expected at least %i columns in result set but received only %i." expectedColumns.Length cursor.FieldCount
            cursor.Close()
            invalidOp message

        for i = 0 to expectedColumns.Length - 1 do
            let expectedName, expectedType = expectedColumns.[i].ColumnName, expectedColumns.[i].DataType
            let actualName, actualType = cursor.GetName( i), cursor.GetFieldType( i)
                
            //TO DO: add extended property on column to mark enums
            let maybeEnum = expectedType = typeof<string> && actualType = typeof<obj>
            let maybeArray = (expectedType = typeof<Array> || expectedType.IsArray) && (actualType = typeof<Array> || actualType.IsArray)
            let typeless = expectedType = typeof<obj> && actualType = typeof<string>
            let isGeometry = actualType = typeof<NetTopologySuite.Geometries.Geometry>
            if (expectedName <> "" && actualName <> expectedName)
                || (actualType <> expectedType && not (maybeArray || maybeEnum) && not typeless && not isGeometry)
            then 
                let message = sprintf """Expected column "%s" of type "%A" at position %i (0-based indexing) but received column "%s" of type "%A".""" expectedName expectedType i actualName actualType
                cursor.Close()
                invalidOp message

    static member internal AsyncExecuteDataReaderTask (cfg, cmd, connection) = Unsafe.uply {
        do! setupConnection (cmd, connection)
        let readerBehavior = getReaderBehavior (connection, cfg)

        if cfg.Prepare then
            do! cmd.PrepareAsync ()

        let! cursor = cmd.ExecuteReaderAsync readerBehavior
        return cursor :?> NpgsqlDataReader }

    static member internal AsyncExecuteReader (cfg, cmd, connection, executionType) =
        mapTask (ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, connection), executionType)

    static member internal LoadDataTable (cursor: Common.DbDataReader) cmd (columns: DataColumn[]) =
        let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd)

        for c in columns do
            CloneDataColumn c |> result.Columns.Add

        result.Load cursor
        result

    static member internal AsyncExecuteDataTables (cfg, cmd, connection, executionType) =
        let t = Unsafe.uply {
            use! cursor = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, connection)

            // No explicit NextResult calls, Load takes care of it
            let results =
                cfg.ResultSets
                |> Array.map (fun resultSet ->
                    if Array.isEmpty resultSet.ExpectedColumns then
                        null
                    else
                        ISqlCommandImplementation.VerifyOutputColumns(cursor, resultSet.ExpectedColumns)
                        ISqlCommandImplementation.LoadDataTable cursor (cmd.Clone()) resultSet.ExpectedColumns |> box)

            ISqlCommandImplementation.SetNumberOfAffectedRows (results, cmd.Statements)
            return results }

        mapTask (t, executionType)

    static member internal AsyncExecuteDataTable (cfg, cmd, connection, executionType) =
        let t = Unsafe.uply {
            use! reader = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, connection) 
            return ISqlCommandImplementation.LoadDataTable reader (cmd.Clone()) cfg.ResultSets.[0].ExpectedColumns }

        mapTask (t, executionType)

    static member internal ExecuteSingle<'TItem> () = Func<_, _, _, _>(fun reader resultSetDefinition cfg -> Unsafe.uply {
        let! xs = MapRowValues<'TItem> (reader, cfg.ResultType, resultSetDefinition)

        return
            if cfg.SingleRow then
                ResizeArrayToOption xs |> box
            elif cfg.CollectionType = CollectionType.Array then
                xs.ToArray () |> box
            elif cfg.CollectionType = CollectionType.List then
                ResizeArrayToList xs |> box
            else
                box xs })
            
    static member internal AsyncExecuteList<'TItem> () = fun (cfg, cmd, connection, executionType) ->
        if cfg.CollectionType = CollectionType.LazySeq && not cfg.SingleRow then
            let t = Unsafe.uply {
                let! reader = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, connection)
                
                let xs =
                    if cfg.ResultSets.[0].ExpectedColumns.Length > 1 then
                        MapRowValuesOntoTupleLazy<'TItem> (reader, cfg.ResultType, cfg.ResultSets.[0])
                    else
                        MapRowValuesLazy<'TItem> (reader, cfg.ResultSets.[0])

                return new LazySeq<'TItem> (xs, reader, cmd) }

            mapTask (t, executionType)
        else
            let xs = Unsafe.uply {
                use! reader = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, connection)
                return! MapRowValues<'TItem> (reader, cfg.ResultType, cfg.ResultSets.[0]) }

            if cfg.SingleRow then
                let t = Unsafe.uply {
                    let! xs = xs
                    return ResizeArrayToOption xs
                }
                mapTask (t, executionType)
            elif cfg.CollectionType = CollectionType.Array then
                let t = Unsafe.uply {
                    let! xs = xs
                    return xs.ToArray ()
                }
                mapTask (t, executionType)
            elif cfg.CollectionType = CollectionType.List then
                let t = Unsafe.uply {
                    let! xs = xs
                    return ResizeArrayToList xs
                }
                mapTask (t, executionType)
            else
                mapTask (xs, executionType)

    static member private ReadResultSet (cursor: Common.DbDataReader, resultSetDefinition, cfg) =
        let func =
            let mutable x = Unchecked.defaultof<_>
            if executeSingleCache.TryGetValue (resultSetDefinition.ErasedRowType, &x) then
                x
            else
                // Verifying the output columns are as expected only during the first call
                // In a clustered DB environment, instances can theoretically differ in this regard, but it's too much of an edge case to have this check on every call
                ISqlCommandImplementation.VerifyOutputColumns(cursor, resultSetDefinition.ExpectedColumns)
                let func = 
                    typeof<ISqlCommandImplementation>
                        .GetMethod(nameof ISqlCommandImplementation.ExecuteSingle, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(resultSetDefinition.ErasedRowType)
                        .Invoke(null, [||]) :?> Func<_, _, _, Ply.Ply<obj>>

                executeSingleCache.[resultSetDefinition.ErasedRowType] <- func
                func

        func.Invoke (cursor, resultSetDefinition, cfg)

    static member internal AsyncExecuteMulti (cfg, cmd, connection, executionType) =
        let t = Unsafe.uply {
            use! cursor = ISqlCommandImplementation.AsyncExecuteDataReaderTask (cfg, cmd, connection)
            let results = Array.zeroCreate cmd.Statements.Count

            // Command contains at least one query
            if cfg.ResultSets |> Array.exists (fun x -> Array.isEmpty x.ExpectedColumns |> not) then
                let mutable go = true

                while go do
                    let currentStatement = GetStatementIndex.Invoke cursor
                    let! res = ISqlCommandImplementation.ReadResultSet (cursor, cfg.ResultSets.[currentStatement], cfg)
                    results.[currentStatement] <- res
                    let! more = cursor.NextResultAsync ()
                    go <- more

            ISqlCommandImplementation.SetNumberOfAffectedRows (results, cmd.Statements)
            return results }

        mapTask (t, executionType)

    static member internal AsyncExecuteNonQuery (cfg, cmd, connection, executionType) = 
        let t = Unsafe.uply {
            do! setupConnection (cmd, connection)
            let readerBehavior = getReaderBehavior (connection, cfg)
            use _ = if readerBehavior.HasFlag CommandBehavior.CloseConnection then cmd.Connection else null

            if cfg.Prepare then
                do! cmd.PrepareAsync ()

            return! cmd.ExecuteNonQueryAsync () }

        mapTask (t, executionType)

    static member internal SetNumberOfAffectedRows (results: obj[], statements: System.Collections.Generic.IReadOnlyList<NpgsqlBatchCommand>) =
        for i in 0 .. statements.Count - 1 do
            if isNull results.[i] then
                results.[i] <- int statements.[i].Rows |> box
