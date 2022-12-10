namespace FSharp.Data.Npgsql

open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Npgsql.DesignTime
open System.IO
open Npgsql

[<TypeProvider>]
type NpgsqlProviders(config) as this = 
    inherit TypeProviderForNamespaces (config, assemblyReplacementMap = [("FSharp.Data.Npgsql.DesignTime", Path.GetFileNameWithoutExtension(config.RuntimeAssembly))], addDefaultProbingLocation = true)
    
    do 
        // register extension mappings
        Npgsql.NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite() |> ignore
    
        let assembly = Assembly.GetExecutingAssembly()
        let assemblyName = assembly.GetName().Name
        let nameSpace = this.GetType().Namespace
        
        assert (typeof<ProvidedCommand>.Assembly.GetName().Name = assemblyName) 

        this.AddNamespace (nameSpace, [ NpgsqlConnectionProvider.getProviderType (assembly, nameSpace) ])