namespace FSharp.Data.Npgsql

open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Npgsql.DesignTime
open System.IO
open Npgsql
open FSharp.Compiler.Syntax
open FSharp.Compiler.Text
open FSharp.Compiler.Xml
open FSharp.Compiler.SyntaxTrivia

type DeriveIsAttribute () =
    inherit System.Attribute ()

type DerivePrintAttribute () =
    inherit System.Attribute ()

type GenerateDbTableTypesAttribute (_connectionString: string) =
    inherit System.Attribute ()

module P =
    let memberFlags isStatic: SynMemberFlags = {
        IsInstance = not isStatic
        IsDispatchSlot = false
        IsOverrideOrExplicitImpl = false
        IsFinal = false
        GetterOrSetterIsCompilerGenerated = false
        MemberKind = SynMemberKind.Member
    }

    let m0 = typeof<Range>.GetMethod("MakeSynthetic", BindingFlags.Instance ||| BindingFlags.NonPublic).Invoke(Range.Zero, [||]) :?> range

    let dottedIdToSynLongId (id: string) =
        let idents =
            id.Split '.'
            |> Array.map (fun x -> Ident (x, m0))
            |> Array.toList

        SynLongIdent (idents, [ for _ in 1 .. idents.Length - 1 -> m0 ], [])

    let exprLongId (ids: string list) =
        let dots = List.init (ids.Length - 1) (fun _ -> m0)
        let ids =
            ids
            |> List.map (fun x -> Ident (x, m0))
        SynExpr.LongIdent (false, SynLongIdent (ids, dots, []), None, m0)

    let compInfo typName =
        SynComponentInfo ([], None, [], [ Ident (typName, m0) ], PreXmlDoc.Empty, false, None, m0)

    let prop name (this: string option) body =
        let flags = memberFlags this.IsNone
        let curriedInfos =
            match this with
            | None -> [ [] ]
            | _ -> [ [ SynArgInfo ([], false, None) ]; [] ]

        let valData = SynValData (Some flags, SynValInfo (curriedInfos, SynArgInfo ([], false, None)), None)
        let id = [
            match this with
            | Some this -> Ident (this, m0)
            | _ -> ()
            
            Ident (name, m0)  
        ]
        let headPat = SynPat.LongIdent (SynLongIdent (id, [], []), None, None, SynArgPats.Pats [], None, m0)
        let bin = SynBinding (None, SynBindingKind.Normal, false, false, [], PreXmlDoc.Empty, valData, headPat, None, body, m0, DebugPointAtBinding.NoneAtInvisible, SynBindingTrivia.Zero)

        SynMemberDefn.Member (bin, m0)

    let field name typ = SynField ([], false, Some (Ident (name, m0)), typ, false, PreXmlDoc.Empty, None, m0, SynFieldTrivia.Zero)

    let record name fields =
        let fields =
            fields
            |> List.map (fun (name, typ) -> field name typ)

        let repr = SynTypeDefnRepr.Simple (SynTypeDefnSimpleRepr.Record (None, fields, m0), m0)
        SynTypeDefn (compInfo name, repr, [], None, m0, SynTypeDefnTrivia.Zero)

    let union name cases =
        let cases =
            cases
            |> List.map (fun (name, _fields) ->
                SynUnionCase ([], SynIdent (Ident (name, m0), None), SynUnionCaseKind.Fields [], PreXmlDoc.Empty, None, m0, { BarRange = None }))

        let repr = SynTypeDefnRepr.Simple (SynTypeDefnSimpleRepr.Union (None, cases, m0), m0)
        SynTypeDefn (compInfo name, repr, [], None, m0, SynTypeDefnTrivia.Zero)

    let match' expr clauses =
        SynExpr.Match (DebugPointAtBinding.NoneAtInvisible, expr, clauses, m0, { MatchKeyword = m0; WithKeyword = m0 })


open P

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces (config, assemblyReplacementMap = [("FSharp.Data.Npgsql.DesignTime", Path.GetFileNameWithoutExtension(config.RuntimeAssembly))], addDefaultProbingLocation = true)

    static let mutable db = Unchecked.defaultof<_>
    
    do 
        // register extension mappings
        Npgsql.NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite() |> ignore
    
        let assembly = Assembly.GetExecutingAssembly()
        let assemblyName = assembly.GetName().Name
        let nameSpace = this.GetType().Namespace
        
        assert (typeof<FSharp.Data.Npgsql.ProvidedCommand>.Assembly.GetName().Name = assemblyName) 

        this.AddNamespace (nameSpace, [ NpgsqlConnectionProvider.getProviderType (assembly, nameSpace) ])

    member _.GenerateDbTableTypes (vals: obj list): SynModuleDecl list =
        match vals with
        | [ :? string as c ] ->
            if isNull (box db) then
                db <- InformationSchema.getDbSchemaLookups c

            let schema = db.Schemas.["public"]

            let ts = [
                for KeyValue (_, enum) in schema.Enums do
                    enum.Values
                    |> Array.map (fun v -> v, [])
                    |> Array.toList
                    |> union enum.Name

                for KeyValue (table, columns) in schema.Tables do
                    let fields =
                        columns
                        |> Seq.map (fun x ->
                            let typ =
                                if x.ClrType.IsArray then
                                    let typName =
                                        if x.DataType.IsUserDefinedType && schema.Enums.ContainsKey x.DataType.Name then
                                            x.DataType.Name
                                        else
                                            x.ClrType.GetElementType().FullName

                                    SynType.Array (1, SynType.LongIdent (dottedIdToSynLongId typName), m0)
                                else
                                    let typName =
                                        if x.DataType.IsUserDefinedType && schema.Enums.ContainsKey x.DataType.Name then
                                            x.DataType.Name
                                        else
                                            x.ClrType.FullName

                                    SynType.LongIdent (dottedIdToSynLongId typName)

                            x.Name, typ)
                        |> Seq.toList

                    record table.Name fields
            ]
            [ SynModuleDecl.Types (ts, m0) ]
        | _ -> []

    member _.DeriveIs (SynTypeDefn (info, repr, members, ctor, m, trivia)) =
        let members =
            match repr with
            | SynTypeDefnRepr.Simple (SynTypeDefnSimpleRepr.Union (unionCases = cases), _) ->
                cases
                |> List.map (fun (SynUnionCase (ident = SynIdent (ident = id))) ->
                    match' (SynExpr.Ident (Ident ("x", m0))) [
                        SynMatchClause (SynPat.LongIdent (dottedIdToSynLongId id.idText, None, None, SynArgPats.Pats [ SynPat.Wild m0 ], None, m0), None, SynExpr.Const (SynConst.Bool true, m0), m0, DebugPointAtTarget.No, SynMatchClauseTrivia.Zero)
                        SynMatchClause (SynPat.Wild m0, None, SynExpr.Const (SynConst.Bool false, m0), m0, DebugPointAtTarget.No, SynMatchClauseTrivia.Zero)
                    ]
                    |> prop $"Is{id.idText}'" (Some "x")
                )
            | _ -> []
            
            |> List.append members

        SynTypeDefn (info, repr, members, ctor, m, trivia)

    member _.DerivePrint (SynTypeDefn (info, repr, members, ctor, m, trivia)) =
        let fieldsParts fields =
            let sep = SynInterpolatedStringPart.String (", ", m0)

            let content = ResizeArray<_> ()

            for i, SynField (idOpt = id; fieldType = _typ) in List.indexed fields do
                let id = id |> Option.map (fun x -> x.idText) |> Option.defaultValue $"Field{i}"

                SynInterpolatedStringPart.String ($"{id} = ", m0)
                |> content.Add

                SynInterpolatedStringPart.FillExpr (exprLongId [ "x"; id ], None)
                |> content.Add

                content.Add sep

            content.RemoveAt (content.Count - 1)
            content

        let members =
            match repr with
            | SynTypeDefnRepr.Simple (SynTypeDefnSimpleRepr.Record (recordFields = fields), _) ->
                let print =
                    let start = SynInterpolatedStringPart.String ("{ ", m0)
                    let end' = SynInterpolatedStringPart.String (" }", m0) 

                    SynExpr.InterpolatedString ([ start; yield! fieldsParts fields; end' ], SynStringKind.Regular, m0)
                    |> prop "Print" (Some "x")

                print :: members
            | SynTypeDefnRepr.Simple (SynTypeDefnSimpleRepr.Union (unionCases = cases), _) ->
                let print =
                    let m =
                        let clauses =
                            cases
                            |> List.map (fun (SynUnionCase (ident = SynIdent (ident = id); caseType = SynUnionCaseKind.Fields fields)) ->
                                let pat, expr =
                                    match fields with
                                    | [] ->
                                        SynPat.LongIdent (SynLongIdent ([ id ], [], []), None, None, SynArgPats.Pats [], None, m0),
                                        SynExpr.Const (SynConst.String ($"{id.idText} {{ }}", SynStringKind.Regular, m0), m0)
                                    | _ ->
                                        let argsIds =
                                            fields
                                            |> List.mapi (fun i (SynField (idOpt = id)) ->
                                                let id = id |> Option.map (fun x -> x.idText) |> Option.defaultValue $"Field{i}"
                                                SynPat.Named (SynIdent (Ident (id, m0), None), false, None, m0), id)

                                        let start = SynInterpolatedStringPart.String ($"{id} {{ ", m0)
                                        let end' = SynInterpolatedStringPart.String (" }", m0)
                                        let sep = SynInterpolatedStringPart.String (", ", m0)

                                        let content = ResizeArray<_> ()

                                        for id in argsIds |> List.unzip |> snd do
                                            SynInterpolatedStringPart.String ($"{id} = ", m0)
                                            |> content.Add

                                            SynInterpolatedStringPart.FillExpr (exprLongId [ id ], None)
                                            |> content.Add

                                            content.Add sep

                                        content.RemoveAt (content.Count - 1)

                                        SynPat.LongIdent (SynLongIdent ([ id ], [], []), None, None, SynArgPats.Pats [ SynPat.Paren (SynPat.Tuple (false, List.unzip argsIds |> fst, [], m0), m0) ], None, m0),
                                        SynExpr.InterpolatedString ([ start; yield! content; end' ], SynStringKind.Regular, m0)

                                SynMatchClause (pat, None, expr, m0, DebugPointAtTarget.No, SynMatchClauseTrivia.Zero))

                        match' (SynExpr.Ident (Ident ("x", m0))) clauses

                    m
                    |> prop "Print" (Some "x")

                print :: members
            | _ -> members

        SynTypeDefn (info, repr, members, ctor, m, trivia)