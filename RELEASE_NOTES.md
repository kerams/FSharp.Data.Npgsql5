### 3.0.0-alpha-5 - April 23rd, 2023
- Allow the use of Npgsql 8
- Remove Ply dependency
- Remove synchronous and `async` commands - only tasks are supported
- Run-time optimizations
  - Use background tasks
  - Refactor internal locking and caching
  - Remove allocations connected to command parameters
  - Remove code verifying that result sets returned at run time match the design-time schema

### 2.1.0 - July 16th, 2022
- Allow the use of Npgsql 7

### 2.0.0 - March 6th, 2022
- Require Npgsql 6 and use the new [raw mode](https://www.roji.org/parameters-batching-and-sql-rewriting) when a command consists of a single statement

### 1.1.0 - March 17th, 2021
- Rows for `ResultType.Record` are now erased to a tuple instead of `obj[]`. This results in faster property access and makes it possible to read value types from Npgsql with fewer allocations.
- Users of PostGIS are now required to set up a global type mapper for NetTopologySuite in their startup code.
  ```fsharp
  open type Npgsql.NpgsqlNetTopologySuiteExtensions

  Npgsql.NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite () |> ignore
  ```

### 1.0.1 - March 9th, 2021
- Minor performance optimizations

### 1.0.0 - February 18th, 2021
- Fixed SingleRow for multiple result sets