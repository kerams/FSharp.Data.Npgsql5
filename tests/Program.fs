﻿module Program

open System
open NpgsqlConnectionTests
open Npgsql

[<EntryPoint>]
let main _ =
    use cmd = DvdRental.CreateCommand<"begin;delete from film where film_id = -5000;end;">(connectionString)
    printfn "%A" (cmd.TaskAsyncExecute().Result)
  

    0

