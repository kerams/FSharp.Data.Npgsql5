module internal FSharp.Data.Npgsql.DesignTime.NpgsqlConnectionProvider

open System
open System.Data
open FSharp.Quotations
open ProviderImplementation.ProvidedTypes
open Npgsql
open FSharp.Data.Npgsql
open InformationSchema
open System.Collections.Concurrent
open System.Reflection
open System.Threading.Tasks

let methodsCache = ConcurrentDictionary<string, ProvidedMethod> ()
let typeCache = ConcurrentDictionary<string, ProvidedTypeDefinition> ()
let schemaCache = ConcurrentDictionary<string, DbSchemaLookups> ()

let addCreateCommandMethod(connectionString, rootType: ProvidedTypeDefinition, commands: ProvidedTypeDefinition, customTypes: Map<string, ProvidedTypeDefinition>,
                           dbSchemaLookups: DbSchemaLookups, globalXCtor, globalPrepare: bool, providedTypeReuse, globalCollectionType: CollectionType) = 
        
    let staticParams = 
        [
            yield ProvidedStaticParameter("CommandText", typeof<string>)
            yield ProvidedStaticParameter("ResultType", typeof<ResultType>, ResultType.Records)
            yield ProvidedStaticParameter("CollectionType", typeof<CollectionType>, globalCollectionType)
            yield ProvidedStaticParameter("SingleRow", typeof<bool>, false)
            yield ProvidedStaticParameter("AllParametersOptional", typeof<bool>, false)
            yield ProvidedStaticParameter("TypeName", typeof<string>, "")
            if not globalXCtor then yield ProvidedStaticParameter("XCtor", typeof<bool>, false)
            yield ProvidedStaticParameter("Prepare", typeof<bool>, globalPrepare)
        ]

    let m = ProvidedMethod("CreateCommand", [], typeof<obj>, isStatic = true)
    m.DefineStaticParameters(staticParams, (fun methodName args ->
        methodsCache.GetOrAdd(
            rootType.Name + methodName,
            fun _ ->
                let sqlStatement, resultType, collectionType, singleRow, allParametersOptional, typename, xctor, (prepare: bool) = 
                    if not globalXCtor then
                        args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, args.[5] :?> _, args.[6] :?> _, args.[7] :?> _
                    else
                        args.[0] :?> _ , args.[1] :?> _, args.[2] :?> _, args.[3] :?> _, args.[4] :?> _, args.[5] :?> _, true, args.[6] :?> _
                        
                if singleRow && not (resultType = ResultType.Records || resultType = ResultType.Tuples) then
                    invalidArg "SingleRow" "SingleRow can be set only for ResultType.Records or ResultType.Tuples."

                let parameters, statements, rawMode = InformationSchema.extractParametersAndOutputColumns(connectionString, sqlStatement, resultType, allParametersOptional, dbSchemaLookups)

                if collectionType = CollectionType.LazySeq && (resultType = ResultType.Records || resultType = ResultType.Tuples) && statements |> List.filter (fun (_, x) -> match x with Query _ -> true | _ -> false) |> List.length > 1 then
                    invalidArg "CollectionType" "LazySeq can only be used when the command returns a single result set. Use a different collection type or rewrite the command so that it returns just the result set that you want to load lazily."

                let statements =
                    statements |> List.mapi (fun i (sql, statementType) ->
                        QuotationsFactory.GetOutputTypes (
                            rootType.Name,
                            sql,
                            statementType,
                            customTypes,
                            resultType,
                            collectionType,
                            singleRow,
                            (if statements.Length > 1 then (i + 1).ToString () else ""),
                            providedTypeReuse))

                let commandTypeName =
                    if typename <> "" then
                        typename
                    else methodName.Replace("=", "").Replace("@", "").Replace("CreateCommand,CommandText", "")

                let sqlStatement =
                    if rawMode then
                        (parameters |> List.indexed |> List.fold (fun (currS: string) (i, param) -> currS.Replace("@" + param.Name, "$" + (i + 1).ToString())) sqlStatement).Trim ()
                    else
                        sqlStatement.Trim ()

                let method =
                    match statements with
                    | [ { Type = NonQuery } ] ->
                        let cmdProvidedType = ProvidedTypeDefinition (commandTypeName, Some typeof<ProvidedCommandNonQuery>, hideObjectMethods = true)
                        commands.AddMember cmdProvidedType
                        let executeArgs = QuotationsFactory.GetExecuteArgs (parameters, customTypes)
                        let m = QuotationsFactory.AddGeneratedMethod (typeof<ProvidedCommandNonQuery>, parameters, executeArgs, None, typeof<Task<int>>, nameof Unchecked.defaultof<ProvidedCommandNonQuery>.ExecuteNonQuery, rawMode)
                        m.AddXmlDoc "Returns the number of rows affected."
                        cmdProvidedType.AddMember m

                        QuotationsFactory.GetCreateCommandMethodNonQuery (cmdProvidedType, prepare, xctor, methodName, sqlStatement)
                    | [ { Type = Query columns } ] when resultType <> ResultType.DataReader && resultType <> ResultType.DataTable ->
                        let cmdProvidedType = ProvidedTypeDefinition (commandTypeName, Some typeof<ProvidedCommandSingleStatement>, hideObjectMethods = true)
                        commands.AddMember cmdProvidedType

                        QuotationsFactory.AddTopLevelTypes cmdProvidedType typeof<ProvidedCommandSingleStatement> parameters singleRow resultType collectionType customTypes statements
                            (if resultType <> ResultType.Records || providedTypeReuse = NoReuse then cmdProvidedType else rootType) rawMode

                        let designTimeConfig = 
                            Expr.Lambda (
                                Var ("x", typeof<unit>),
                                Expr.Call (typeof<DesignTimeConfigSingleStatement>.GetMethod (nameof DesignTimeConfigSingleStatement.Create, BindingFlags.Static ||| BindingFlags.Public), [
                                    Expr.Value $"""{if collectionType = CollectionType.LazySeq then "1" else "0"}|{int resultType}|{if prepare then "1" else "0"}"""
                                    Expr.NewArray (typeof<DataColumn>, columns |> List.map (fun x -> x.ToDataColumnExpr true))
                                ]))

                        QuotationsFactory.GetCommandFactoryMethod (cmdProvidedType, typeof<ProvidedCommandSingleStatement>, designTimeConfig, xctor, methodName, sqlStatement)
                    | _ ->
                        let cmdProvidedType = ProvidedTypeDefinition (commandTypeName, Some typeof<ProvidedCommand>, hideObjectMethods = true)
                        commands.AddMember cmdProvidedType

                        QuotationsFactory.AddTopLevelTypes cmdProvidedType typeof<ProvidedCommand> parameters singleRow resultType collectionType customTypes statements
                            (if resultType <> ResultType.Records || providedTypeReuse = NoReuse then cmdProvidedType else rootType) rawMode

                        let designTimeConfig = 
                            Expr.Lambda (
                                Var ("x", typeof<unit>),
                                Expr.Call (typeof<DesignTimeConfig>.GetMethod (nameof DesignTimeConfig.Create, BindingFlags.Static ||| BindingFlags.Public), [
                                    Expr.Value $"""{int resultType}|{int collectionType}|{if prepare then "1" else "0"}|{if singleRow then "1" else "0"}"""
                                    QuotationsFactory.BuildDataColumnsExpr (statements, resultType <> ResultType.DataTable)
                                ]))

                        QuotationsFactory.GetCommandFactoryMethod (cmdProvidedType, typeof<ProvidedCommand>, designTimeConfig, xctor, methodName, sqlStatement)

                rootType.AddMember method
                method)
    ))
    rootType.AddMember m

let createTableTypes(customTypes : Map<string, ProvidedTypeDefinition>, item: DbSchemaLookupItem) = 
    let tables = ProvidedTypeDefinition("Tables", Some typeof<obj>)
    tables.AddMembersDelayed <| fun() ->
        
        item.Tables
        |> Seq.map (fun s -> 
            let tableName = s.Key.Name
            let description = s.Key.Description
            let columns = s.Value |> List.ofSeq

            //type data row
            let dataRowType = QuotationsFactory.GetDataRowType(customTypes, columns)
            //type data table
            let dataTableType = 
                QuotationsFactory.GetDataTableType(
                    tableName,
                    dataRowType,
                    customTypes,
                    columns
                )

            dataTableType.AddMember dataRowType
        
            do
                description |> Option.iter (fun x -> dataTableType.AddXmlDoc( sprintf "<summary>%s</summary>" x))

            do //ctor
                let invokeCode _ = 

                    let columnExprs = [ for c in columns -> c.ToDataColumnExpr false ]

                    let twoPartTableName = 
                        use x = new NpgsqlCommandBuilder()
                        sprintf "%s.%s" (x.QuoteIdentifier item.Schema.Name) (x.QuoteIdentifier tableName)

                    let columns = columns |> List.map(fun c ->  c.Name) |> String.concat " ,"
                    let cmdText = sprintf "SELECT %s FROM %s" columns twoPartTableName

                    <@@ 
                        let selectCommand = new NpgsqlCommand(cmdText)
                        let table = new FSharp.Data.Npgsql.DataTable<DataRow>(selectCommand)
                        table.TableName <- twoPartTableName
                        table.Columns.AddRange(%%Expr.NewArray(typeof<DataColumn>, columnExprs))
                        table
                    @@>

                let ctor = ProvidedConstructor([], invokeCode)
                dataTableType.AddMember ctor    

                let binaryImport = 
                    ProvidedMethod(
                        "BinaryImport", 
                        [
                            ProvidedParameter ("connection", typeof<NpgsqlConnection>)
                            ProvidedParameter ("ignoreIdentityColumns", typeof<bool>)
                        ],
                        typeof<uint64>,
                        fun args -> Expr.Call (typeof<Utils>.GetMethod (nameof Utils.BinaryImport), [ Expr.Coerce (args.[0], typeof<DataTable<DataRow>>); args.[1]; args.[2] ])
                    )
                dataTableType.AddMember binaryImport

            dataTableType
        )
        |> List.ofSeq

    tables

let createRootType (assembly, nameSpace: string, typeName, connectionString, xctor, prepare, reuseProvidedTypes, collectionType) =
    if String.IsNullOrWhiteSpace connectionString then invalidArg "Connection" "Value is empty!" 
        
    let databaseRootType = ProvidedTypeDefinition (assembly, nameSpace, typeName, baseType = Some typeof<obj>, hideObjectMethods = true)
    let schemaLookups = schemaCache.GetOrAdd (connectionString, InformationSchema.getDbSchemaLookups)
    
    let dbSchemas = schemaLookups.Schemas
                    |> Seq.map (fun s -> ProvidedTypeDefinition(s.Key, baseType = Some typeof<obj>, hideObjectMethods = true))
                    |> List.ofSeq
                  
    databaseRootType.AddMembers dbSchemas
    
    let customTypes =
        [ for schemaType in dbSchemas do
            let es = ProvidedTypeDefinition("Types", Some typeof<obj>, hideObjectMethods = true)
            for (KeyValue(_, enum)) in schemaLookups.Schemas.[schemaType.Name].Enums do
                let t = ProvidedTypeDefinition(enum.Name, Some typeof<string>, hideObjectMethods = true, nonNullable = true)
                for value in enum.Values do t.AddMember(ProvidedField.Literal(value, t, value))
                es.AddMember t
                let udtTypeName = sprintf "%s.%s" enum.Schema enum.Name
                yield udtTypeName, t
            schemaType.AddMember es
        ] |> Map.ofList
        
    for schemaType in dbSchemas do
        schemaType.AddMemberDelayed <| fun () ->
            createTableTypes(customTypes, schemaLookups.Schemas.[schemaType.Name])

    let commands = ProvidedTypeDefinition("Commands", None)
    databaseRootType.AddMember commands
    let providedTypeReuse = if reuseProvidedTypes then WithCache typeCache else NoReuse
    addCreateCommandMethod (connectionString, databaseRootType, commands, customTypes, schemaLookups, xctor, prepare, providedTypeReuse, collectionType)

    databaseRootType

let internal getProviderType (assembly, nameSpace) = 

    let providerType = ProvidedTypeDefinition (assembly, nameSpace, "NpgsqlConnection", Some typeof<obj>, hideObjectMethods = true)

    providerType.DefineStaticParameters (
        [ 
            ProvidedStaticParameter("ConnectionString", typeof<string>) 
            ProvidedStaticParameter("XCtor", typeof<bool>, false) 
            ProvidedStaticParameter("Prepare", typeof<bool>, false)
            ProvidedStaticParameter("ReuseProvidedTypes", typeof<bool>, false) 
            ProvidedStaticParameter("CollectionType", typeof<CollectionType>, CollectionType.List)
        ],
        fun typeName args -> typeCache.GetOrAdd (typeName, fun typeName -> createRootType (assembly, nameSpace, typeName, unbox args.[0], unbox args.[1], unbox args.[2], unbox args.[3], unbox args.[4])))

    providerType.AddXmlDoc """
<summary>Typed access to PostgreSQL programmable objects, tables and functions.</summary> 
<param name='ConnectionString'>String used to open a PostgreSQL database at design-time to generate types.</param>
<param name='XCtor'>If set, commands will accept an NpgsqlConnection and an optional NpgsqlTransaction instead of a connection string.</param>
<param name='Prepare'>If set, commands will be executed as prepared. See Npgsql documentation for prepared statements.</param>
<param name='ReuseProvidedTypes'>Reuse the return type for commands that select data of identical shape. Please see the readme for details.</param>
<param name='CollectionType'>Indicates whether rows should be returned in a list, array or ResizeArray.</param>
"""
    providerType


 

