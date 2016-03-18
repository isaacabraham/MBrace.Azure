﻿namespace MBrace.Azure

open System

open MBrace.Azure.Runtime

/// Azure Configuration Builder. Used to specify MBrace.Azure cluster storage configuration.
[<AutoSerializable(true); Sealed; NoEquality; NoComparison>]
type Configuration(storageConnectionString : string) =
    static let storageEnv = "AzureStorageConnectionString"
    static let getEnv (envName:string) =
        let aux found target =
            if String.IsNullOrWhiteSpace found then Environment.GetEnvironmentVariable(envName, target)
            else found

        Array.fold aux null [|EnvironmentVariableTarget.Process; EnvironmentVariableTarget.User; EnvironmentVariableTarget.Machine|]        

    let mutable _storageAccountName = null
    let mutable _storageConnectionString = null

    let parseStorage conn =
        let account = AzureStorageAccount.Parse conn
        _storageConnectionString <- account.ConnectionString
        _storageAccountName <- account.AccountName

    do
        parseStorage storageConnectionString

    let mutable version = typeof<Configuration>.Assembly.GetName().Version

    // Default Queue Storage Configuration
    let mutable workItemQueue        = "MBraceWorkItemQueue"

    // Default Blob Storage Containers
    let mutable runtimeContainer    = "mbraceruntimedata"
    let mutable userDataContainer   = "mbraceuserdata"
    let mutable cloudValueContainer = "mbracecloudvalue"
    let mutable assemblyContainer   = "mbraceassemblies"

    // Default Table Storage tables
    let mutable userDataTable       = "MBraceUserData"
    let mutable runtimeTable        = "MBraceRuntimeData"
    let mutable runtimeLogsTable    = "MBraceRuntimeLogs"

    /// Runtime version this configuration is targeting. Default to current assembly version.
    member __.Version
        with get () = version.ToString()
        and set v = version <- Version.Parse v

    /// Append version to given configuration e.g. $RuntimeQueue$Version. Defaults to true.
    member val UseVersionSuffix    = true with get, set

    /// Runtime identifier, used for runtime isolation when using the same storage/servicebus accounts. Defaults to 0.
    member val SuffixId             = 0us with get, set

    /// Append runtime id to given configuration e.g. $RuntimeQueue$Version$Id. Defaults to false.
    member val UseSuffixId          = false with get, set

    /// Specifies wether the cluster should optimize closure serialization. Defaults to true.
    member val OptimizeClosureSerialization = true with get, set

    // #region Credentials

    /// Azure Storage account name
    member __.StorageAccount = _storageAccountName

    /// Azure Storage connection string.
    member __.StorageConnectionString
        with get () = _storageConnectionString
        and set scs = parseStorage scs

    /// Storage work item queue used by the runtime.
    member __.WorkItemQueue
        with get () = workItemQueue
        and set rq = Validate.queueName rq ; workItemQueue <- rq

    /// Azure Storage container used by the runtime.
    member __.RuntimeContainer
        with get () = runtimeContainer
        and set rc = Validate.containerName rc ; runtimeContainer <- rc

    /// Azure Storage container used for user data.
    member __.UserDataContainer
        with get () = userDataContainer
        and set udc = Validate.containerName udc ; userDataContainer <- udc

    /// Azure Storage container used for Vagabond assembly dependencies.
    member __.AssemblyContainer
        with get () = assemblyContainer
        and set ac = Validate.containerName ac ; assemblyContainer <- ac

    /// Azure Storage container used for CloudValue persistence.
    member __.CloudValueContainer
        with get () = cloudValueContainer
        and set cvc = Validate.containerName cvc ; cloudValueContainer <- cvc

    // #region Table Storage

    /// Azure Storage table used by the runtime.
    member __.RuntimeTable
        with get () = runtimeTable
        and set rt = Validate.tableName rt; runtimeTable <- rt

    /// Azure Storage table used by the runtime for storing logs.
    member __.RuntimeLogsTable
        with get () = runtimeLogsTable
        and set rlt = Validate.tableName rlt ; runtimeLogsTable <- rlt

    /// Azure Storage table used for user data.
    member __.UserDataTable
        with get () = userDataTable
        and set udt = Validate.tableName udt ; userDataTable <- udt

    /// Gets or sets or the local environment Azure storage connection string
    static member EnvironmentStorageConnectionString
        with get () = 
            match getEnv storageEnv with
            | null | "" -> invalidOp "unset Azure Storage connection string environment variable."
            | conn -> conn

        and set conn =
            let _ = AzureStorageAccount.Parse conn
            Environment.SetEnvironmentVariable(storageEnv, conn, EnvironmentVariableTarget.User)
            Environment.SetEnvironmentVariable(storageEnv, conn, EnvironmentVariableTarget.Process)

    /// Creates a configuration object by reading connection string information from the local environment variables.
    static member FromEnvironmentVariables() =
        new Configuration(Configuration.EnvironmentStorageConnectionString)