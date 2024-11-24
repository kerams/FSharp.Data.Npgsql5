namespace FSharp.Data.Npgsql.DesignTime

open System
open System.Data
open System.Reflection
open FSharp.Quotations
open ProviderImplementation.ProvidedTypes
open Npgsql
open FSharp.Data.Npgsql
open InformationSchema
open System.Collections.Concurrent
open System.Threading.Tasks

type internal ReturnType = {
    Single: Type
    RowProvidedType: Type option
}

type internal Statement = {
    Type: StatementType
    ReturnType: ReturnType option
    Sql: string
}

type internal QuotationsFactory () = 
    static let (|Arg3|) xs = 
        assert (List.length xs = 3)
        Arg3(xs.[0], xs.[1], xs.[2])

    static let (|Arg6|) xs = 
        assert (List.length xs = 6)
        Arg6(xs.[0], xs.[1], xs.[2], xs.[3], xs.[4], xs.[5])

    static let (|Arg7|) xs = 
        assert (List.length xs = 7)
        Arg7(xs.[0], xs.[1], xs.[2], xs.[3], xs.[4], xs.[5], xs.[6])

    static let defaultCommandTimeout =
        use cmd = new NpgsqlCommand ()
        cmd.CommandTimeout

    static member val GetValueAtIndexExpr: (Expr * int) -> Expr =
        let mi = typeof<Unit>.Assembly.GetType("Microsoft.FSharp.Core.LanguagePrimitives+IntrinsicFunctions").GetMethod("GetArray").MakeGenericMethod typeof<obj>
        fun (arrayExpr, index) -> Expr.Call (mi, [ arrayExpr; Expr.Value index ])

    static member val DataColumnArrayEmptyExpr =
        let mi = typeof<Array>.GetMethod(nameof Array.Empty, BindingFlags.Static ||| BindingFlags.Public).MakeGenericMethod typeof<DataColumn>
        Expr.Call (mi, [])

    static member GetNullableValueFromDataRow (t: Type, name: string) (exprArgs: Expr list) =
        Expr.Call (typeof<Utils>.GetMethod(nameof Utils.GetNullableValueFromDataRow).MakeGenericMethod t, [
            exprArgs.[0]
            Expr.Value name ])

    static member SetNullableValueInDataRow (t: Type, name: string) (exprArgs: Expr list) =
        Expr.Call (typeof<Utils>.GetMethod(nameof Utils.SetNullableValueInDataRow).MakeGenericMethod t, [
            exprArgs.[0]
            Expr.Value name
            Expr.Coerce (exprArgs.[1], typeof<obj>) ])

    static member GetNonNullableValueFromDataRow (name: string) (exprArgs: Expr list) =
        Expr.Call (exprArgs.Head, typeof<DataRow>.GetMethod ("get_Item", [| typeof<string> |]), [ Expr.Value name ])

    static member SetNonNullableValueInDataRow (name: string) (exprArgs: Expr list) =
        Expr.Call (exprArgs.Head, typeof<DataRow>.GetMethod ("set_Item", [| typeof<string>; typeof<obj> |]), [ Expr.Value name; Expr.Coerce (exprArgs.[1], typeof<obj>) ])
    
    static member GetMapperFromOptionToObj (t: Type, value: Expr) =
        Expr.Call (typeof<Utils>.GetMethod(nameof Utils.OptionToObj).MakeGenericMethod t, [ Expr.Coerce (value, typeof<obj>) ])

    static member Sequentials continuation exprs =
        match exprs with
        | [] -> continuation
        | h :: t -> Expr.Sequential (h, QuotationsFactory.Sequentials continuation t)

    static member AddGeneratedMethod (providedCommandType: Type, sqlParameters: Parameter list, executeArgs: ProvidedParameter list, genericParameter, providedOutputType, name, rawMode) =
        let createParamNonGeneric param paramExpr =
            let paramVar = Var ("p", typeof<NpgsqlParameter>)
            let paramVarExpr = Expr.Var paramVar

            Expr.Let (
                paramVar,
                Expr.NewObject (typeof<NpgsqlParameter>.GetConstructor [| |], []),
                [
                    if param.Precision <> 0uy then
                        Expr.PropertySet (paramVarExpr, typeof<NpgsqlParameter>.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.Scale), Expr.Value param.Scale)

                    if param.Scale <> 0uy then
                        Expr.PropertySet (paramVarExpr, typeof<NpgsqlParameter>.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.Precision), Expr.Value param.Precision)

                    if not rawMode then
                        Expr.PropertySet (paramVarExpr, typeof<NpgsqlParameter>.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.ParameterName), Expr.Value param.Name)

                    if not param.DataType.IsFixedLength && param.MaxLength > 0 then
                        Expr.PropertySet (paramVarExpr, typeof<NpgsqlParameter>.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.Size), Expr.Value param.MaxLength)

                    Expr.PropertySet (paramVarExpr, typeof<NpgsqlParameter>.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.NpgsqlDbType), Expr.Value param.NpgsqlDbType) 

                    if param.Optional then
                        Expr.Call (typeof<Utils>.GetMethod(nameof Utils.SetOptionalParamValue).MakeGenericMethod param.DataType.ClrType, [ paramVarExpr; paramExpr ])
                    else
                        Expr.PropertySet (paramVarExpr, typeof<NpgsqlParameter>.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.Value), Expr.Coerce (paramExpr, typeof<obj>))
                ]
                |> QuotationsFactory.Sequentials paramVarExpr
            )

        let createParamGeneric param paramExpr =
            let paramType = typedefof<NpgsqlParameter<_>>.MakeGenericType [| param.DataType.ClrType |]
            let newParam = Expr.NewObject (paramType.GetConstructor [| typeof<string>; param.DataType.ClrType |], [ Expr.Value (if rawMode then null else param.Name); paramExpr ])

            let requiresExplicitType =
                match param.NpgsqlDbType with
                | NpgsqlTypes.NpgsqlDbType.Oid
                | NpgsqlTypes.NpgsqlDbType.Oidvector
                | NpgsqlTypes.NpgsqlDbType.Xid
                | NpgsqlTypes.NpgsqlDbType.Cid
                | NpgsqlTypes.NpgsqlDbType.Bit
                | NpgsqlTypes.NpgsqlDbType.Money
                | NpgsqlTypes.NpgsqlDbType.Char
                | NpgsqlTypes.NpgsqlDbType.InternalChar
                | NpgsqlTypes.NpgsqlDbType.Name
                | NpgsqlTypes.NpgsqlDbType.Citext
                | NpgsqlTypes.NpgsqlDbType.Xml
                | NpgsqlTypes.NpgsqlDbType.Json
                | NpgsqlTypes.NpgsqlDbType.Jsonb -> true
                | _ -> false

            if param.Precision <> 0uy || param.Scale <> 0uy || requiresExplicitType then
                let paramVar = Var ("p", paramType)
                let paramVarExpr = Expr.Var paramVar

                Expr.Let (
                    paramVar,
                    newParam,
                    [
                        if param.Precision <> 0uy then
                            Expr.PropertySet (paramVarExpr, paramType.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.Precision), Expr.Value param.Precision)

                        if param.Scale <> 0uy then
                            Expr.PropertySet (paramVarExpr, paramType.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.Scale), Expr.Value param.Scale)

                        if requiresExplicitType then
                            Expr.PropertySet (paramVarExpr, paramType.GetProperty (nameof Unchecked.defaultof<NpgsqlParameter>.NpgsqlDbType), Expr.Value param.NpgsqlDbType)
                    ]
                    |> QuotationsFactory.Sequentials paramVarExpr
                )
            else
                newParam

        let addParams paramsExpr parameters continuation =
            parameters
            |> List.map (fun (param, paramExpr) ->
                let newParam =
                    if param.DataType.ClrType.IsArray || param.Optional || param.DataType.IsUserDefinedType then
                        createParamNonGeneric param paramExpr
                    else
                        createParamGeneric param paramExpr

                Expr.Call (paramsExpr, typeof<NpgsqlParameterCollection>.GetMethod (nameof Unchecked.defaultof<NpgsqlParameterCollection>.Add, [| typeof<NpgsqlParameter> |]), [ newParam ])
            )
            |> QuotationsFactory.Sequentials continuation

        let invokeCode (exprArgs: Expr list) =
            let method =
                match genericParameter with
                | None -> providedCommandType.GetMethod name
                | Some (t: Type) -> providedCommandType.GetMethod(name).MakeGenericMethod t

            if exprArgs.Length > 1 then
                let var = Var ("x", providedCommandType)
                let paramsVar = Var ("ps", typeof<NpgsqlParameterCollection>)

                Expr.Let (
                    var,
                    exprArgs.[0],
                    Expr.Let (
                        paramsVar,
                        Expr.PropertyGet (Expr.PropertyGet (Expr.Var var, providedCommandType.GetProperty "NpgsqlCommand"), typeof<NpgsqlCommand>.GetProperty (nameof Unchecked.defaultof<NpgsqlCommand>.Parameters, typeof<NpgsqlParameterCollection>)),
                        addParams (Expr.Var paramsVar) (List.zip sqlParameters exprArgs.Tail) (Expr.Call (Expr.Var var, method, []))
                    )
                )
            else
                Expr.Call (exprArgs.[0], method, [])

        ProvidedMethod("TaskAsyncExecute", executeArgs, providedOutputType, invokeCode)

    static member GetRecordType (rootTypeName, columns: Column list, customTypes: Map<string, ProvidedTypeDefinition>, typeNameSuffix, providedTypeCache: ConcurrentDictionary<string, ProvidedTypeDefinition>) =
        columns 
        |> List.groupBy (fun x -> x.Name)
        |> List.iter (fun (name, xs) ->
            if not xs.Tail.IsEmpty then
                failwithf "Non-unique column name %s is not supported for ResultType.Records." name
            if String.IsNullOrEmpty name then
                failwithf "One or more columns do not have a name. Please give the columns an explicit alias.")
        
        let createType typeName =
            let baseType = ProvidedTypeBuilder.MakeTupleType (columns |> List.sortBy (fun x -> x.Name) |> List.map (fun x -> x.MakeProvidedType customTypes))
            let recordType = ProvidedTypeDefinition (typeName, baseType = Some baseType, hideObjectMethods = true)

            columns
            |> List.sortBy (fun x -> x.Name)
            |> List.iteri (fun i col ->
                let rec accessor instance (baseType: Type) tupleIndex coerce =
                    if tupleIndex < 7 then
                        Expr.PropertyGet (
                            (if coerce then Expr.Coerce (instance, baseType) else instance),
                            baseType.GetProperty (sprintf "Item%d" (tupleIndex + 1))
                        )
                    else
                        let constituentTuple = baseType.GetGenericArguments().[7]
                        let rest =
                            Expr.PropertyGet (
                                (if coerce then Expr.Coerce (instance, baseType) else instance),
                                baseType.GetProperty "Rest")
                        accessor rest constituentTuple (tupleIndex - 7) false

                ProvidedProperty (col.Name, col.MakeProvidedType customTypes, fun args -> accessor args.[0] baseType i true) |> recordType.AddMember)

            recordType

        let typeName = columns |> List.map (fun x ->
            let t = if Map.containsKey x.DataType.FullName customTypes then x.DataType.FullName else x.ClrType.Name
            if x.Nullable then sprintf "%s:Option<%s>" x.Name t else sprintf "%s:%s" x.Name t) |> List.sort |> String.concat ", "

        providedTypeCache.GetOrAdd (rootTypeName + typeName, fun _ -> createType typeName)

    static member GetDataRowPropertyGetterAndSetterCode (column: Column) =
        let name = column.Name
        if column.Nullable then
            let setter = if column.ReadOnly then None else Some (QuotationsFactory.SetNullableValueInDataRow (column.ClrType, name))
            QuotationsFactory.GetNullableValueFromDataRow (column.ClrType, name), setter
        else
            let setter = if column.ReadOnly then None else Some (QuotationsFactory.SetNonNullableValueInDataRow name)
            QuotationsFactory.GetNonNullableValueFromDataRow name, setter

    static member GetDataRowType (customTypes: Map<string, ProvidedTypeDefinition>, columns: Column list) = 
        let rowType = ProvidedTypeDefinition("Row", Some typeof<DataRow>)
            
        columns 
        |> List.mapi(fun i col ->

            if col.Name = "" then failwithf "Column #%i doesn't have a name. Please use an explicit alias." (i + 1)

            let propertyType = col.MakeProvidedType(customTypes)

            let getter, setter = QuotationsFactory.GetDataRowPropertyGetterAndSetterCode col

            let p = ProvidedProperty(col.Name, propertyType, getter, ?setterCode = setter)

            if col.Description <> "" then p.AddXmlDoc col.Description

            p
        )
        |> rowType.AddMembers

        rowType

    static member GetDataTableType
        (
            typeName, 
            dataRowType: ProvidedTypeDefinition,
            customTypes: Map<string, ProvidedTypeDefinition>,
            outputColumns: Column list
        ) =

        let tableType = ProvidedTypeDefinition(typeName, Some( ProvidedTypeBuilder.MakeGenericType(typedefof<_ DataTable>, [ dataRowType ])))
      
        do //Columns
            let columnsType = ProvidedTypeDefinition("Columns", Some typeof<DataColumnCollection>)
            tableType.AddMember columnsType
            let columns = ProvidedProperty("Columns", columnsType, getterCode = fun args -> <@@ (%%args.Head: DataTable<DataRow>).Columns @@>)
            tableType.AddMember columns
      
            for column in outputColumns do
                let propertyType = ProvidedTypeDefinition(column.Name, Some typeof<DataColumn>)
                columnsType.AddMember propertyType

                let property = 
                    let columnName = column.Name
                    ProvidedProperty(column.Name, propertyType, getterCode = fun args -> <@@ (%%args.Head: DataColumnCollection).[columnName] @@>)
            
                columnsType.AddMember property

        do
            let parameters, updateableColumns = 
                [ 
                    for c in outputColumns do 
                        if not c.ReadOnly
                        then 
                            let dataType = c.MakeProvidedType(customTypes, forceNullability = c.OptionalForInsert)
                            let parameter = 
                                if c.OptionalForInsert
                                then ProvidedParameter(c.Name, parameterType = dataType, optionalValue = null)
                                else ProvidedParameter(c.Name, dataType)

                            yield parameter, c
                ] 
                |> List.unzip

            let methodXmlDoc = 
                String.concat "\n" [
                    for c in updateableColumns do
                        if c.Description <> "" 
                        then 
                            let defaultConstrain = 
                                if c.HasDefaultConstraint 
                                then sprintf " Default constraint: %s." c.DefaultConstraint
                                else ""
                            yield sprintf "<param name='%s'>%s%s</param>" c.Name c.Description defaultConstrain
                ]


            let invokeCode = fun (args: _ list)-> 

                let argsValuesConverted = 
                    (args.Tail, updateableColumns)
                    ||> List.map2 (fun valueExpr c ->
                        if c.OptionalForInsert
                        then 
                            QuotationsFactory.GetMapperFromOptionToObj(c.ClrType, valueExpr)
                        else
                            valueExpr
                    )

                <@@ 
                    let table: DataTable<DataRow> = %%args.[0]
                    let row = table.NewRow()

                    let values: obj[] = %%Expr.NewArray(typeof<obj>, [ for x in argsValuesConverted -> Expr.Coerce(x, typeof<obj>) ])
                    let namesOfUpdateableColumns: string[] = %%Expr.NewArray(typeof<string>, [ for c in updateableColumns -> Expr.Value(c.Name) ])

                    for name, value in Array.zip namesOfUpdateableColumns values do 
                        if not(Convert.IsDBNull(value)) 
                        then 
                            row.[name] <- value
                    row
                @@>

            do 
                let newRowMethod = ProvidedMethod("NewRow", parameters, dataRowType, invokeCode)
                if methodXmlDoc <> "" then newRowMethod.AddXmlDoc methodXmlDoc
                tableType.AddMember newRowMethod

                let addRowMethod =
                    ProvidedMethod(
                        "AddRow", 
                        parameters, 
                        typeof<Void>, 
                        invokeCode = fun args ->
                            let newRow = invokeCode args
                            <@@ (%%args.[0]: DataTable<DataRow>).Rows.Add(%%newRow) @@>
                    )

                if methodXmlDoc <> "" then addRowMethod.AddXmlDoc methodXmlDoc
                tableType.AddMember addRowMethod

        do
            let commonParams = [
                ProvidedParameter("batchSize", typeof<int>, optionalValue = 1)
                ProvidedParameter("continueUpdateOnError", typeof<bool>, optionalValue = false) 
                ProvidedParameter("conflictOption", typeof<ConflictOption>, optionalValue = ConflictOption.OverwriteChanges) 
                ProvidedParameter("batchTimeout", typeof<int>, optionalValue = defaultCommandTimeout) 
            ]

            tableType.AddMembers [

                ProvidedMethod(
                    "Update", 
                    ProvidedParameter("dataSource", typeof<NpgsqlDataSource>) :: commonParams, 
                    typeof<int>,
                    fun (Arg6(table, dataSource, batchSize, continueUpdateOnError, conflictOption, batchTimeout)) -> 
                        <@@ 
                            let conn = (%%dataSource: NpgsqlDataSource).CreateConnection ()
                            Utils.UpdateDataTable(%%table, conn, null, %%batchSize, %%continueUpdateOnError, %%conflictOption, %%batchTimeout)
                        @@>
                )

                ProvidedMethod(
                    "Update", 
                    ProvidedParameter("connection", typeof<NpgsqlConnection> ) 
                    :: ProvidedParameter("transaction", typeof<NpgsqlTransaction>, optionalValue = null) 
                    :: commonParams, 
                    typeof<int>,
                    fun (Arg7(table, conn, tx, batchSize, continueUpdateOnError, conflictOption, batchTimeout)) -> 
                        <@@ 
                            Utils.UpdateDataTable(%%table, %%conn, %%tx, %%batchSize, %%continueUpdateOnError, %%conflictOption, %%batchTimeout)
                        @@>
                )

            ]

        tableType

    static member GetOutputTypes (rootTypeName, sql, statementType, customTypes: Map<string, ProvidedTypeDefinition>, resultType, collectionType, singleRow, typeNameSuffix, providedTypeReuse) =    
        let returnType =
            match resultType, statementType with
            | ResultType.DataReader, _
            | _, Control ->
                None
            | _, NonQuery ->
                Some { Single = typeof<int>; RowProvidedType = None }
            | ResultType.DataTable, Query columns ->
                let dataRowType = QuotationsFactory.GetDataRowType (customTypes, columns)
                let dataTableType =
                    QuotationsFactory.GetDataTableType(
                        "Table" + typeNameSuffix,
                        dataRowType,
                        customTypes,
                        columns)

                dataTableType.AddMember dataRowType

                Some { Single = dataTableType; RowProvidedType = None }
            | _, Query columns ->
                let providedRowType =
                    if List.length columns = 1 then
                        columns.Head.MakeProvidedType customTypes
                    elif resultType = ResultType.Records then 
                        QuotationsFactory.GetRecordType (rootTypeName, columns, customTypes, typeNameSuffix, providedTypeReuse) :> Type
                    else
                        match columns with
                        | [ x ] -> x.MakeProvidedType customTypes
                        | xs -> Reflection.FSharpType.MakeTupleType [| for x in xs -> x.MakeProvidedType customTypes |]

                Some {
                    Single =
                        if singleRow then
                            ProvidedTypeBuilder.MakeGenericType (typedefof<_ option>, [ providedRowType ])
                        elif collectionType = CollectionType.ResizeArray then
                            ProvidedTypeBuilder.MakeGenericType (typedefof<ResizeArray<_>>, [ providedRowType ])
                        elif collectionType = CollectionType.Array then
                            providedRowType.MakeArrayType ()
                        elif collectionType = CollectionType.LazySeq then
                            ProvidedTypeBuilder.MakeGenericType (typedefof<LazySeq<_>>, [ providedRowType ])
                        else
                            ProvidedTypeBuilder.MakeGenericType (typedefof<_ list>, [ providedRowType ])
                    RowProvidedType = Some providedRowType }

        { Type = statementType; Sql = sql; ReturnType = returnType }

    static member GetExecuteArgs(sqlParameters: Parameter list, customTypes: Map<string, ProvidedTypeDefinition>) = 
        [
            for p in sqlParameters do
                let parameterName = p.Name

                let t =
                    let customType = customTypes.TryFind p.DataType.UdtTypeName
                    
                    match p.DataType.IsUserDefinedType, customType with
                    | true, Some t ->
                        if p.DataType.ClrType.IsArray then t.MakeArrayType() else upcast t
                    | _ -> p.DataType.ClrType

                if p.Optional then
                    ProvidedParameter(
                        parameterName,
                        parameterType = ProvidedTypeBuilder.MakeGenericType(typedefof<_ option>, [ t ]),
                        optionalValue = null
                    )
                else
                    ProvidedParameter(parameterName, parameterType = t)
        ]

    static member GetCreateCommandMethodNonQuery (cmdProvidedType: ProvidedTypeDefinition, prepare: bool, isExtended, methodName, sqlStatement: string) =
        let ctorImpl = typeof<ProvidedCommandNonQuery>.GetConstructors() |> Array.exactlyOne

        if isExtended then
            let body (Arg3 (conn, tran, commandTimeout)) =
                let cmd = Expr.Call (typeof<Utils>.GetMethod (nameof Utils.NpgsqlCommandXCtor), [ conn; tran; Expr.Value sqlStatement; commandTimeout ])
                Expr.NewObject (ctorImpl, [ Expr.Value prepare; cmd ])

            let parameters = [
                ProvidedParameter("connection", typeof<NpgsqlConnection>) 
                ProvidedParameter("transaction", typeof<NpgsqlTransaction>, optionalValue = null)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)
        else
            let body (args: Expr list) =
                let cmd = Expr.Call (typeof<Utils>.GetMethod (nameof Utils.NpgsqlCommand), [ args.[0]; Expr.Value sqlStatement; args.[1] ])
                Expr.NewObject (ctorImpl, [ Expr.Value prepare; cmd ])

            let parameters = [
                ProvidedParameter("dataSource", typeof<NpgsqlDataSource>)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)

    static member GetCommandFactoryMethod (cmdProvidedType: ProvidedTypeDefinition, providedCommandType: Type, designTimeConfig, isExtended, methodName, sqlStatement: string) = 
        let ctorImpl = providedCommandType.GetConstructors() |> Array.exactlyOne

        if isExtended then
            let body (Arg3 (conn, tran, commandTimeout)) =
                let cmd = Expr.Call (typeof<Utils>.GetMethod (nameof Utils.NpgsqlCommandXCtor), [ conn; tran; Expr.Value sqlStatement; commandTimeout ])
                Expr.NewObject (ctorImpl, [ Expr.Value (cmdProvidedType.Name.GetHashCode()); designTimeConfig; cmd ])

            let parameters = [
                ProvidedParameter("connection", typeof<NpgsqlConnection>) 
                ProvidedParameter("transaction", typeof<NpgsqlTransaction>, optionalValue = null)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)
        else
            let body (args: Expr list) =
                let cmd = Expr.Call (typeof<Utils>.GetMethod (nameof Utils.NpgsqlCommand), [ args.[0]; Expr.Value sqlStatement; args.[1] ])
                Expr.NewObject (ctorImpl, [ Expr.Value (cmdProvidedType.Name.GetHashCode()); designTimeConfig; cmd ])

            let parameters = [
                ProvidedParameter("dataSource", typeof<NpgsqlDataSource>)
                ProvidedParameter("commandTimeout", typeof<int>, optionalValue = defaultCommandTimeout) ]

            ProvidedMethod (methodName, parameters, cmdProvidedType, body, true)

    static member AddProvidedTypeToDeclaring resultType returnType (declaringType: ProvidedTypeDefinition) =
        if resultType = ResultType.Records then
            returnType.RowProvidedType
            |> Option.iter (fun x -> if x :? ProvidedTypeDefinition then declaringType.AddMember x)
        elif resultType = ResultType.DataTable && not returnType.Single.IsPrimitive then
            returnType.Single |> declaringType.AddMember

    static member BuildDataColumnsExpr (statements, slimDataColumns, arrayTypeCompat) =
        Expr.NewArray (typeof<DataColumn[]>,
            statements
            |> List.map (fun x ->
                match x.Type with
                | Query columns ->
                    Expr.NewArray (typeof<DataColumn>, columns |> List.map (fun x -> x.ToDataColumnExpr (slimDataColumns, arrayTypeCompat)))
                | _ ->
                    QuotationsFactory.DataColumnArrayEmptyExpr))

    static member AddTopLevelTypes (cmdProvidedType: ProvidedTypeDefinition) (providedCommandType: Type) parameters singleRow resultType collectionType customTypes statements typeToAttachTo rawMode =
        let executeArgs = QuotationsFactory.GetExecuteArgs (parameters, customTypes)
        
        let addMethodRedirects (outputType: Type) =
            let methodName, generic =
                match resultType, statements with
                | ResultType.DataReader, _ -> nameof Unchecked.defaultof<ProvidedCommand>.GetDataReader, None
                | ResultType.DataTable, [ _ ] -> nameof Unchecked.defaultof<ProvidedCommand>.GetDataTable, None
                | ResultType.DataTable, _ -> nameof Unchecked.defaultof<ProvidedCommand>.GetDataTables, None
                | _, [ { Type = Query columns } ] ->
                    let t =
                        match columns with
                        | [ c ] ->
                            if c.Nullable then
                                typedefof<_ option>.MakeGenericType c.ClrType
                            else
                                c.ClrType
                        | _ ->
                            let columns =
                                match resultType with
                                | ResultType.Records -> columns |> List.sortBy (fun c -> c.Name)
                                | ResultType.Tuples -> columns
                                | _ -> []
                            
                            Reflection.FSharpType.MakeTupleType (columns |> List.map (fun c -> if c.Nullable then typedefof<_ option>.MakeGenericType c.ClrType else c.ClrType) |> List.toArray)
                    
                    let methodName =
                        match collectionType with
                        | _ when singleRow -> nameof Unchecked.defaultof<ProvidedCommandSingleStatement>.ExecuteSingleRow
                        | CollectionType.LazySeq -> nameof Unchecked.defaultof<ProvidedCommandSingleStatement>.ExecuteLazySeq
                        | CollectionType.List -> nameof Unchecked.defaultof<ProvidedCommandSingleStatement>.ExecuteList
                        | CollectionType.Array -> nameof Unchecked.defaultof<ProvidedCommandSingleStatement>.ExecuteArray
                        | _ -> nameof Unchecked.defaultof<ProvidedCommandSingleStatement>.ExecuteResizeArray

                    methodName, Some t
                | _ -> nameof Unchecked.defaultof<ProvidedCommand>.ExecuteMultiStatement, None

            QuotationsFactory.AddGeneratedMethod (providedCommandType, parameters, executeArgs, generic, (typedefof<Task<_>>.MakeGenericType outputType), methodName, rawMode)
            |> cmdProvidedType.AddMember

        match statements with
        | _ when resultType = ResultType.DataReader ->
            addMethodRedirects typeof<NpgsqlDataReader>
        | [ { ReturnType = Some returnType; Sql = sql } ] ->
            addMethodRedirects returnType.Single
            QuotationsFactory.AddProvidedTypeToDeclaring resultType returnType typeToAttachTo
        | _ ->
            let resultSetsType = ProvidedTypeDefinition ("ResultSets", baseType = Some typeof<obj[]>, hideObjectMethods = true)

            statements
            |> List.iteri (fun i statement ->
                match statement.Type, statement.ReturnType with
                | NonQuery, _ ->
                    let prop = ProvidedProperty (sprintf "RowsAffected%d" (i + 1), typeof<int>, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce (args.[0], typeof<obj[]>), i))
                    sprintf "Number of rows affected by \"%s\"." statement.Sql |> prop.AddXmlDoc
                    resultSetsType.AddMember prop
                | Query _, Some rt ->
                    let prop = ProvidedProperty (sprintf "ResultSet%d" (i + 1), rt.Single, fun args -> QuotationsFactory.GetValueAtIndexExpr (Expr.Coerce (args.[0], typeof<obj[]>), i))
                    sprintf "Rows returned for query \"%s\"." statement.Sql |> prop.AddXmlDoc
                    resultSetsType.AddMember prop
                | _ -> ()

                statement.ReturnType |> Option.iter (fun rt -> QuotationsFactory.AddProvidedTypeToDeclaring resultType rt typeToAttachTo))

            addMethodRedirects resultSetsType
            cmdProvidedType.AddMember resultSetsType


