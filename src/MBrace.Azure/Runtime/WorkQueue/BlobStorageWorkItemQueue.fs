namespace MBrace.Azure.Runtime

open MBrace.Runtime
open Microsoft.WindowsAzure.Storage.Queue
open MBrace.Core.Internals
open Nessos.FsPickler
open Microsoft.WindowsAzure.Storage
open System.Runtime.Serialization

[<AutoSerializable(true) ; Sealed; DataContract>]
type BlobStorageWorkItemQueue() =
    let enqueue(workItem:CloudWorkItem, isClientSideEnqueue) =
        async {
            return ()
        }

    interface ICloudWorkItemQueue with
        member __.BatchEnqueue(workItems) =
            workItems
            |> Seq.map(fun workItem -> enqueue(workItem, true))
            |> Async.Parallel
            |> Async.Ignore
        member __.Enqueue(workItem, isClientSideEnqueue) =
            enqueue(workItem, isClientSideEnqueue)
        member __.TryDequeue(id) =
            None |> async.Return

[<AutoSerializable(true) ; Sealed; DataContract>]
type StorageCloudQueue<'a>(queue:CloudQueue) =
    let pickler = FsPickler.CreateBinarySerializer()
    interface MBrace.Core.CloudQueue<'a> with
        member __.DequeueAsync timeout = async {
            let! message = queue.GetMessageAsync() |> Async.AwaitTaskCorrect
            return pickler.UnPickle<'a>(message.AsBytes) }
        
        member __.DequeueBatchAsync maxItems = async {
            let! messages = queue.GetMessagesAsync(maxItems) |> Async.AwaitTaskCorrect
            return 
                messages
                |> Seq.map(fun m -> pickler.UnPickle<'a>(m.AsBytes))
                |> Seq.toArray }

        member __.EnqueueAsync item =
            item
            |> pickler.Pickle
            |> CloudQueueMessage
            |> queue.AddMessageAsync
            |> Async.AwaitTaskCorrect
        member this.EnqueueBatchAsync items =
            items
            |> Seq.map((this :> MBrace.Core.CloudQueue<'a>).EnqueueAsync)
            |> Async.Parallel
            |> Async.Ignore
        member __.GetCountAsync() =
            match queue.ApproximateMessageCount.HasValue with
            | false -> 0L
            | true -> int64 queue.ApproximateMessageCount.Value
            |> async.Return
        member __.TryDequeueAsync() = async {
            let! message = queue.GetMessageAsync() |> Async.AwaitTaskCorrect
            return pickler.UnPickle<'a>(message.AsBytes) |> Some }
        member __.Id = queue.Name
        member __.Dispose() = async.Return ()

[<Sealed; DataContract>]
type StorageQueueProvider(connectionString) =
    let queueClient =
        let account = CloudStorageAccount.Parse connectionString
        account.CreateCloudQueueClient()
    interface ICloudQueueProvider with
        member __.CreateQueue<'a> queueId =
            let queueRef = queueClient.GetQueueReference queueId
            async {
                do! queueRef.CreateIfNotExistsAsync() |> Async.AwaitTaskCorrect |> Async.Ignore
                return StorageCloudQueue(queueRef) :> MBrace.Core.CloudQueue<'a>
            }
        member __.GetQueueById queueId =
            let queueRef = queueClient.GetQueueReference queueId
            async { return StorageCloudQueue(queueRef) :> MBrace.Core.CloudQueue<'a> }

        member __.GetRandomQueueName() = failwith "Not implemented yet"
        member __.Id = failwith "Not implemented yet"
        member __.Name = failwith "Not implemented yet"
        