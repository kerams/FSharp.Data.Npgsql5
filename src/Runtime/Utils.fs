namespace FSharp.Data.Npgsql

open System
open System.Data
open System.Data.Common
open System.Runtime.CompilerServices
open System.Collections.Concurrent
open System.ComponentModel
open Npgsql

[<Extension>]
[<AbstractClass; Sealed>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
type Utils private() =
    static let optionCtorCache =
        ConcurrentDictionary<Type, obj -> obj> ()

    static let statementIndexGetter =
        typeof<NpgsqlDataReader>.GetProperty("StatementIndex", Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic).GetMethod
    
    [<Extension>]
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member GetStatementIndex(cursor: DbDataReader) =
        statementIndexGetter.Invoke(cursor, null) :?> int

    static member private CreateOptionType typeParam =
        typeof<unit option>.GetGenericTypeDefinition().MakeGenericType([| typeParam |])
    
    static member private MakeOptionValue (typeParam: Type) v =
        match optionCtorCache.TryGetValue typeParam with
        | true, ctor ->
            ctor v
        | _ ->
            let cases =  Utils.CreateOptionType typeParam |> Reflection.FSharpType.GetUnionCases |> Array.partition (fun x -> x.Name = "Some")
            let someCtor = fst cases |> Array.exactlyOne |> Reflection.FSharpValue.PreComputeUnionConstructor
            let noneCtor = snd cases |> Array.exactlyOne |> Reflection.FSharpValue.PreComputeUnionConstructor
            let noneValue = noneCtor [| |]

            optionCtorCache.GetOrAdd (typeParam, fun v -> if Convert.IsDBNull v then noneValue else someCtor [| v |]) v
    
    [<Extension>]
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member MapRowValues<'TItem>(cursor: DbDataReader, resultType : ResultType, resultSet: ResultSetDefinition, isTypeReuseEnabled) =
        let rowMapping =
            if resultSet.ExpectedColumns.Length = 1 then
                Array.item 0
            elif resultType = ResultType.Tuples then
                let clrTypeName = resultSet.SeqItemTypeName
                Reflection.FSharpValue.PreComputeTupleConstructor(Type.GetType(clrTypeName, throwOnError = true))
            else
                box
        
        seq {
            let values = Array.zeroCreate cursor.FieldCount

            // If type type reuse of records is enabled, columns need to be sorted alphabetically, because records are erased to arrays and thus the insert order
            // of elements matters
            let columns =
                if isTypeReuseEnabled && resultType = ResultType.Records then
                    resultSet.ExpectedColumns |> Array.indexed |> Array.sortBy (fun (_, col) -> col.ColumnName)
                else
                    resultSet.ExpectedColumns |> Array.indexed

            while cursor.Read() do
                cursor.GetValues(values) |> ignore

                columns
                |> Array.map (fun (i, column) ->
                    let obj = values.[i]

                    if column.ExtendedProperties.[SchemaTableColumn.AllowDBNull] :?> bool then
                        let dataTypeName = column.ExtendedProperties.["ClrType.PartiallyQualifiedName"] :?> string
                        let dataType = Type.GetType(dataTypeName, throwOnError = true)
                        Utils.MakeOptionValue dataType obj
                    else
                        obj)
                |> rowMapping
                |> unbox<'TItem>
        }
    
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member DbNull = box DBNull.Value

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member GetMapperWithNullsToOptions(nullsToOptions, mapper: obj[] -> obj) = 
        fun values -> 
            nullsToOptions values
            mapper values

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member SetRef<'t>(r : byref<'t>, arr: (string * obj)[], i) = r <- arr.[i] |> snd |> unbox

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member UpdateDataTable(table: DataTable<DataRow>, connection, transaction, batchSize, continueUpdateOnError, conflictOption, batchTimeout) = 

        if batchSize <= 0 then invalidArg "batchSize" "Batch size has to be larger than 0."
        if batchTimeout <= 0 then invalidArg "batchTimeout" "Batch timeout has to be larger than 0."

        use selectCommand = table.SelectCommand.Clone()

        selectCommand.Connection <- connection
        if transaction <> null then selectCommand.Transaction <- transaction

        for column in table.Columns do column.ExtendedProperties.Remove("ClrType.PartiallyQualifiedName")
        
        use dataAdapter = new BatchDataAdapter(selectCommand, batchTimeout, UpdateBatchSize = int batchSize, ContinueUpdateOnError = continueUpdateOnError)
        use commandBuilder = new CommandBuilder(table, DataAdapter = dataAdapter, ConflictOption = conflictOption)

        use __ = dataAdapter.RowUpdating.Subscribe(fun args ->

            if  args.Errors = null 
                && args.StatementType = Data.StatementType.Insert 
                && dataAdapter.UpdateBatchSize = 1
            then 
                let columnsToRefresh = ResizeArray()
                for c in table.Columns do
                    if c.AutoIncrement  
                        || (c.AllowDBNull && args.Row.IsNull c.Ordinal)
                    then 
                        columnsToRefresh.Add( commandBuilder.QuoteIdentifier c.ColumnName)

                if columnsToRefresh.Count > 0
                then                        
                    let returningClause = columnsToRefresh |> String.concat "," |> sprintf " RETURNING %s"
                    let cmd = args.Command
                    cmd.CommandText <- cmd.CommandText + returningClause
                    cmd.UpdatedRowSource <- UpdateRowSource.FirstReturnedRecord
        )

        dataAdapter.Update(table)   

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    static member BinaryImport(table: DataTable<DataRow>, connection: NpgsqlConnection) = 
        let copyFromCommand = 
            [ for c in table.Columns -> c.ColumnName ]
            |> String.concat ", "
            |> sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" table.TableName

        use writer = connection.BeginBinaryImport(copyFromCommand)

        for row in table.Rows do
            writer.WriteRow(row.ItemArray)

        writer.Complete()
