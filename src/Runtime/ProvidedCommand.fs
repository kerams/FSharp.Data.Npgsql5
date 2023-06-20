namespace rec FSharp.Data.Npgsql

open Npgsql
open System
open System.Data
open System.Collections.Generic
open System.ComponentModel
open System.Reflection
open System.Threading.Tasks
open type Utils

[<assembly: CompilerServices.TypeProviderAssembly("FSharp.Data.Npgsql.DesignTime")>]
do ()

[<EditorBrowsable(EditorBrowsableState.Never); NoEquality; NoComparison>]
type DesignTimeConfig = {
    ResultType: ResultType
    ResultSets: (ResultSetDefinition * Func<NpgsqlDataReader, Task<obj>>)[]
    Prepare: bool
}
    with
        static member Create (stringValues: string, columns: DataColumn[][]) =
            let split = stringValues.Split '|'
            let resultType = int split.[0] |> enum<ResultType>
            let collectionType = int split.[1] |> enum<CollectionType>
            let singleRow = split.[3] = "1"
            {
                ResultType = resultType
                ResultSets =
                    columns
                    |> Array.map (fun r ->
                        let resultSet = CreateResultSetDefinition (r, resultType)
                        let resultSetReader =
                            if (resultType = ResultType.Records || resultType = ResultType.Tuples) && r.Length > 0 then
                                typeof<ProvidedCommand>
                                    .GetMethod(nameof ProvidedCommand.ReadResultSet, BindingFlags.NonPublic ||| BindingFlags.Static)
                                    .MakeGenericMethod(resultSet.ErasedRowType)
                                    .Invoke(null, [| BuildRowReader (resultSet, resultType); singleRow; collectionType |])
                                    :?> Func<NpgsqlDataReader, Task<obj>>
                            else
                                null

                        resultSet, resultSetReader)
                Prepare = split.[2] = "1"
            }

[<EditorBrowsable(EditorBrowsableState.Never); Sealed>]
type ProvidedCommand (commandNameHash: int, cfgBuilder: unit -> DesignTimeConfig, _cmd: NpgsqlCommand) =
    static let cfgCache = Dictionary ()
    static let _lock = obj ()

    let cfg =
        lock _lock (fun () ->
            match cfgCache.TryGetValue commandNameHash with
            | true, cfg -> cfg
            | _ ->
                let cfg = cfgBuilder ()
                cfgCache.[commandNameHash] <- cfg
                cfg)

    member val NpgsqlCommand = _cmd

    interface IDisposable with
        member x.Dispose () = x.NpgsqlCommand.Dispose ()

    member x.GetDataReader () = backgroundTask {
        let openHere = x.NpgsqlCommand.Connection.State = ConnectionState.Closed

        if openHere then
            do! x.NpgsqlCommand.Connection.OpenAsync ()

        if cfg.Prepare then
            do! x.NpgsqlCommand.PrepareAsync ()

        let behavior =
            if cfg.ResultType = ResultType.DataTable then CommandBehavior.KeyInfo else CommandBehavior.Default
            ||| if openHere then CommandBehavior.CloseConnection else CommandBehavior.Default

        let! cursor = x.NpgsqlCommand.ExecuteReaderAsync behavior
        return cursor :?> NpgsqlDataReader }

    static member internal LoadDataTable (cursor: NpgsqlDataReader) cmd (columns: DataColumn[]) =
        let result = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand = cmd)

        for c in columns do
            CloneDataColumn c |> result.Columns.Add

        result.Load cursor
        result

    member x.GetDataTables () =
        backgroundTask {
            use! cursor = x.GetDataReader ()

            // No explicit NextResult calls, Load takes care of it
            let results =
                cfg.ResultSets
                |> Array.map (fun (resultSet, _) ->
                    if Array.isEmpty resultSet.ExpectedColumns then
                        null
                    else
                        ProvidedCommand.LoadDataTable cursor (x.NpgsqlCommand.Clone ()) resultSet.ExpectedColumns |> box)

            ProvidedCommand.SetNumberOfAffectedRows (results, x.NpgsqlCommand.Statements)
            return results
        }

    member x.GetDataTable () =
        backgroundTask {
            use! reader = x.GetDataReader () 
            return ProvidedCommand.LoadDataTable reader (x.NpgsqlCommand.Clone ()) ((fst cfg.ResultSets.[0]).ExpectedColumns)
        }

    static member internal ReadResultSet<'TItem> (rowReader, singleRow, collectionType) = Func<NpgsqlDataReader, Task<obj>>(fun reader -> backgroundTask {
        let! xs = MapRowValues<'TItem> (reader, rowReader)

        return
            match collectionType with
            | _ when singleRow ->
                ResizeArrayToOption xs |> box
            | CollectionType.Array ->
                xs.ToArray () |> box
            | CollectionType.List ->
                ResizeArrayToList xs |> box
            | _ ->
                box xs })

    member x.ExecuteMultiStatement () =
        backgroundTask {
            use! cursor = x.GetDataReader ()
            let results = Array.zeroCreate x.NpgsqlCommand.Statements.Count

            // Command contains at least one query
            let mutable go = cfg.ResultSets |> Array.exists (fun (resultSet, _) -> resultSet.ExpectedColumns.Length > 0)

            while go do
                let currentStatement = GetStatementIndex.Invoke cursor
                let! res = (snd cfg.ResultSets.[currentStatement]).Invoke cursor
                results.[currentStatement] <- res
                let! more = cursor.NextResultAsync ()
                go <- more

            ProvidedCommand.SetNumberOfAffectedRows (results, x.NpgsqlCommand.Statements)
            return results
        }

    static member private SetNumberOfAffectedRows (results: obj[], statements: System.Collections.Generic.IReadOnlyList<NpgsqlBatchCommand>) =
        for i in 0 .. statements.Count - 1 do
            if isNull results.[i] then
                results.[i] <- int statements.[i].Rows |> box

[<EditorBrowsable(EditorBrowsableState.Never); Sealed>]
type ProvidedCommandNonQuery (prepare: bool, _cmd: NpgsqlCommand) =
    member val NpgsqlCommand = _cmd

    interface IDisposable with
        member x.Dispose () = x.NpgsqlCommand.Dispose ()

    member x.ExecuteNonQuery () = 
        backgroundTask {
            let openHere = x.NpgsqlCommand.Connection.State = ConnectionState.Closed

            if openHere then
                do! x.NpgsqlCommand.Connection.OpenAsync ()

            use _ = if openHere then x.NpgsqlCommand.Connection else null

            if prepare then
                do! x.NpgsqlCommand.PrepareAsync ()

            return! x.NpgsqlCommand.ExecuteNonQueryAsync ()
        }

[<EditorBrowsable(EditorBrowsableState.Never); NoEquality; NoComparison>]
type DesignTimeConfigSingleStatement = {
    Dispose: bool
    RowReader: Func<NpgsqlDataReader, obj>
    Prepare: bool
}
    with
        static member Create (stringValues: string, columns: DataColumn[]) =
            let split = stringValues.Split '|'
            let resultType = int split.[1] |> enum
            {
                Dispose = split.[0] = "1"
                RowReader = BuildRowReader (CreateResultSetDefinition (columns, resultType), resultType)
                Prepare = split.[2] = "1"
            }

[<EditorBrowsable(EditorBrowsableState.Never); Sealed>]
type ProvidedCommandSingleStatement (commandNameHash: int, cfgBuilder: unit -> DesignTimeConfigSingleStatement, _cmd: NpgsqlCommand) =
    static let cfgCache = Dictionary ()
    static let _lock = obj ()

    let cfg =
        lock _lock (fun () ->
            match cfgCache.TryGetValue commandNameHash with
            | true, cfg -> cfg
            | _ ->
                let cfg = cfgBuilder ()
                cfgCache.[commandNameHash] <- cfg
                cfg)

    member val NpgsqlCommand = _cmd

    interface IDisposable with
        member x.Dispose () =
            if cfg.Dispose then
                x.NpgsqlCommand.Dispose ()

    member private x.GetDataReader () = backgroundTask {
        let openHere = x.NpgsqlCommand.Connection.State = ConnectionState.Closed

        if openHere then
            do! x.NpgsqlCommand.Connection.OpenAsync ()

        if cfg.Prepare then
            do! x.NpgsqlCommand.PrepareAsync ()

        let! cursor = x.NpgsqlCommand.ExecuteReaderAsync (if openHere then CommandBehavior.CloseConnection else CommandBehavior.Default)
        return cursor :?> NpgsqlDataReader }

    member x.ExecuteLazySeq<'TItem> () =
        backgroundTask {
            let! reader = x.GetDataReader ()
            let xs = MapRowValuesLazy<'TItem> (reader, cfg.RowReader)
            return new LazySeq<'TItem> (xs, reader, x.NpgsqlCommand)
        }
    
    member x.ExecuteResizeArray<'TItem> () =
        backgroundTask {
            use! reader = x.GetDataReader ()
            return! MapRowValues<'TItem> (reader, cfg.RowReader)
        }

    member x.ExecuteArray<'TItem> () =
        backgroundTask {
            use! reader = x.GetDataReader ()
            let! res = MapRowValues<'TItem> (reader, cfg.RowReader)
            return res.ToArray ()
        }

    member x.ExecuteList<'TItem> () =
        backgroundTask {
            use! reader = x.GetDataReader ()
            let! res = MapRowValues<'TItem> (reader, cfg.RowReader)
            return ResizeArrayToList res
        }

    member x.ExecuteSingleRow<'TItem> () =
        backgroundTask {
            use! reader = x.GetDataReader ()
            let! res = MapRowValues<'TItem> (reader, cfg.RowReader)
            return ResizeArrayToOption res
        }