module Program

open System
open NpgsqlConnectionTests
open Npgsql

[<EntryPoint>]
let main _ =
    use cmd = DvdRental.CreateCommand<"delete from actor where false; select 1::bigint where false", SingleRow = true>(connectionString)
    let r = cmd.TaskAsyncExecute().Result
    printfn "%A" (r.RowsAffected1)
    printfn "%A" (r.ResultSet2)
  

    0

