namespace FSharp.Data.Npgsql

open System
open System.Data
open Npgsql
open System.ComponentModel
open System.Reflection
open System.Collections.Concurrent
open System.Threading.Tasks
open type Utils

[<assembly: CompilerServices.TypeProviderAssembly("FSharp.Data.Npgsql.DesignTime")>]
do ()

[<EditorBrowsable(EditorBrowsableState.Never)>]
type CollectionTypeInt =
    | List = 0
    | Array = 1
    | ResizeArray = 2
    | LazySeq = 3
    | Option = 4

[<EditorBrowsable(EditorBrowsableState.Never); NoEquality; NoComparison>]
type DesignTimeConfig = {
    ResultType: ResultType
    CollectionType: CollectionTypeInt
    ResultSets: ResultSetDefinition[]
    Prepare: bool
}
    with
        static member Create (stringValues: string, columns: DataColumn[][]) =
            let split = stringValues.Split '|'
            let resultType = int split.[0] |> enum<ResultType>
            {
                ResultType = resultType
                CollectionType = int split.[1] |> enum
                ResultSets = columns |> Array.map (fun r -> CreateResultSetDefinition (r, resultType))
                Prepare = split.[2] = "1"
            }

[<EditorBrowsable(EditorBrowsableState.Never); Sealed>]
type ProvidedCommand (commandNameHash: int, cfgBuilder: unit -> DesignTimeConfig, _cmd: NpgsqlCommand) =
    static let cfgCache = ConcurrentDictionary ()
    static let executeSingleCache = ConcurrentDictionary ()

    let cfg =
        let mutable x = Unchecked.defaultof<_>
        if cfgCache.TryGetValue (commandNameHash, &x) then
            x
        else
            let cfg = cfgBuilder ()
            cfgCache.[commandNameHash] <- cfg
            cfg

    static let getReaderBehavior (closeConnection, cfg) = 
        // Don't pass CommandBehavior.SingleRow to Npgsql, because it only applies to the first row of the first result set and all other result sets are completely ignored
        if cfg.CollectionType = CollectionTypeInt.Option && cfg.ResultSets.Length = 1 then CommandBehavior.SingleRow else CommandBehavior.Default
        ||| if cfg.ResultType = ResultType.DataTable then CommandBehavior.KeyInfo else CommandBehavior.Default
        ||| if closeConnection then CommandBehavior.CloseConnection else CommandBehavior.Default

    member val NpgsqlCommand = _cmd

    interface IDisposable with
        member x.Dispose () =
            if cfg.CollectionType <> CollectionTypeInt.LazySeq then
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

    member x.GetDataReader () = task {
        let openHere = x.NpgsqlCommand.Connection.State = ConnectionState.Closed

        if openHere then
            do! x.NpgsqlCommand.Connection.OpenAsync ()

        if cfg.Prepare then
            do! x.NpgsqlCommand.PrepareAsync ()

        let! cursor = x.NpgsqlCommand.ExecuteReaderAsync (getReaderBehavior (openHere, cfg))
        return cursor :?> NpgsqlDataReader }

    static member internal LoadDataTable (cursor: Common.DbDataReader) cmd (columns: DataColumn[]) =
        let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd)

        for c in columns do
            CloneDataColumn c |> result.Columns.Add

        result.Load cursor
        result

    member x.GetDataTables () =
        task {
            use! cursor = x.GetDataReader ()

            // No explicit NextResult calls, Load takes care of it
            let results =
                cfg.ResultSets
                |> Array.map (fun resultSet ->
                    if Array.isEmpty resultSet.ExpectedColumns then
                        null
                    else
                        ProvidedCommand.VerifyOutputColumns(cursor, resultSet.ExpectedColumns)
                        ProvidedCommand.LoadDataTable cursor (x.NpgsqlCommand.Clone()) resultSet.ExpectedColumns |> box)

            ProvidedCommand.SetNumberOfAffectedRows (results, x.NpgsqlCommand.Statements)
            return results
        }

    member x.GetDataTable () =
        task {
            use! reader = x.GetDataReader () 
            return ProvidedCommand.LoadDataTable reader (x.NpgsqlCommand.Clone()) cfg.ResultSets.[0].ExpectedColumns
        }

    static member internal ExecuteSingle<'TItem> () = Func<_, _, _, _>(fun reader resultSetDefinition cfg -> task {
        let! xs = MapRowValues<'TItem> (reader, cfg.ResultType, resultSetDefinition)

        return
            match cfg.CollectionType with
            | CollectionTypeInt.Option ->
                ResizeArrayToOption xs |> box
            | CollectionTypeInt.Array ->
                xs.ToArray () |> box
            | CollectionTypeInt.List ->
                ResizeArrayToList xs |> box
            | _ ->
                box xs })
            
    member x.ExecuteSingleStatement<'TItem> () =
        if cfg.CollectionType = CollectionTypeInt.LazySeq then
            task {
                let! reader = x.GetDataReader ()
                
                let xs =
                    if cfg.ResultSets.[0].ExpectedColumns.Length > 1 then
                        MapRowValuesOntoTupleLazy<'TItem> (reader, cfg.ResultType, cfg.ResultSets.[0])
                    else
                        MapRowValuesLazy<'TItem> (reader, cfg.ResultSets.[0])

                return new LazySeq<'TItem> (xs, reader, x.NpgsqlCommand)
            }
            |> box
        else
            let xs = task {
                use! reader = x.GetDataReader ()
                return! MapRowValues<'TItem> (reader, cfg.ResultType, cfg.ResultSets.[0]) }

            match cfg.CollectionType with
            | CollectionTypeInt.Option ->
                task {
                    let! xs = xs
                    return ResizeArrayToOption xs
                }
                |> box
            | CollectionTypeInt.Array ->
                task {
                    let! xs = xs
                    return xs.ToArray ()
                }
                |> box
            | CollectionTypeInt.List ->
                task {
                    let! xs = xs
                    return ResizeArrayToList xs
                }
                |> box
            | _ ->
                box xs

    static member private ReadResultSet (cursor: Common.DbDataReader, resultSetDefinition, cfg) =
        let func =
            let mutable x = Unchecked.defaultof<_>
            if executeSingleCache.TryGetValue (resultSetDefinition.ErasedRowType, &x) then
                x
            else
                // Verifying the output columns are as expected only during the first call
                // In a clustered DB environment, instances can theoretically differ in this regard, but it's too much of an edge case to have this check on every call
                ProvidedCommand.VerifyOutputColumns(cursor, resultSetDefinition.ExpectedColumns)
                let func = 
                    typeof<ProvidedCommand>
                        .GetMethod(nameof ProvidedCommand.ExecuteSingle, BindingFlags.NonPublic ||| BindingFlags.Static)
                        .MakeGenericMethod(resultSetDefinition.ErasedRowType)
                        .Invoke(null, [||]) :?> Func<_, _, _, Task<obj>>

                executeSingleCache.[resultSetDefinition.ErasedRowType] <- func
                func

        func.Invoke (cursor, resultSetDefinition, cfg)

    member x.ExecuteMultiStatement () =
        task {
            use! cursor = x.GetDataReader ()
            let results = Array.zeroCreate x.NpgsqlCommand.Statements.Count

            // Command contains at least one query
            if cfg.ResultSets |> Array.exists (fun x -> Array.isEmpty x.ExpectedColumns |> not) then
                let mutable go = true

                while go do
                    let currentStatement = GetStatementIndex.Invoke cursor
                    let! res = ProvidedCommand.ReadResultSet (cursor, cfg.ResultSets.[currentStatement], cfg)
                    results.[currentStatement] <- res
                    let! more = cursor.NextResultAsync ()
                    go <- more

            ProvidedCommand.SetNumberOfAffectedRows (results, x.NpgsqlCommand.Statements)
            return results
        }

    static member internal SetNumberOfAffectedRows (results: obj[], statements: System.Collections.Generic.IReadOnlyList<NpgsqlBatchCommand>) =
        for i in 0 .. statements.Count - 1 do
            if isNull results.[i] then
                results.[i] <- int statements.[i].Rows |> box

[<EditorBrowsable(EditorBrowsableState.Never); Sealed>]
type ProvidedCommandNonQuery (prepare: bool, _cmd: NpgsqlCommand) =
    member val NpgsqlCommand = _cmd

    interface IDisposable with
        member x.Dispose () = x.NpgsqlCommand.Dispose ()

    member x.ExecuteNonQuery () = 
        task {
            let openHere = x.NpgsqlCommand.Connection.State = ConnectionState.Closed

            if openHere then
                do! x.NpgsqlCommand.Connection.OpenAsync ()

            use _ = if openHere then x.NpgsqlCommand.Connection else null

            if prepare then
                do! x.NpgsqlCommand.PrepareAsync ()

            return! x.NpgsqlCommand.ExecuteNonQueryAsync ()
        }

[<EditorBrowsable(EditorBrowsableState.Never); Sealed>]
type ProvidedCommandSingleStatement (dispose: bool, prepare: bool, _cmd: NpgsqlCommand) =
    member val NpgsqlCommand = _cmd

    interface IDisposable with
        member x.Dispose () =
            if dispose then
                x.NpgsqlCommand.Dispose ()
