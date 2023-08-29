using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventConsumerClient
{
    public class EventProcessor
    {
        ConcurrentDictionary<string, int> partitionEventCount = new ConcurrentDictionary<string, int>();

        public async Task StartEventProcessing(CancellationToken cToken)
        {
            string consumerGroup = "YOUR_CONSUMER_GROUP_NAME";
            var storageClient = new BlobContainerClient(
                "YOUR_STORAGE_ACCOUNT_CONNECTION_STRING",
                "YOUR_STORAGE_ACCOUNT_CONTAINER_NAME");

            var eventHubConnectionString = "YOUR_EVENT_HUB_CONNECTION_STRING";
            var eventHubName = "YOUR_EVENT_HUB_NAME"; // known as topic in kafka
            var processor = new EventProcessorClient(storageClient, consumerGroup, eventHubConnectionString, eventHubName);


            processor.ProcessEventAsync += HandleEventProcessing;
            processor.ProcessErrorAsync += HandleEventError;

            try
            {
                await processor.StartProcessingAsync(cToken);
                await Task.Delay(Timeout.Infinite, cToken);
            }
            catch(Exception ex) 
            {
                Console.WriteLine($"Something wrong in StartEventProcessing {ex.Message}");
            }
            finally
            {
                await processor.StopProcessingAsync();
                processor.ProcessEventAsync -= HandleEventProcessing;
                processor.ProcessErrorAsync -= HandleEventError;
            }

        }

        async Task HandleEventProcessing(ProcessEventArgs args)
        {
            try
            {
                if (args.CancellationToken.IsCancellationRequested)
                {
                    return;
                }

                string partition = args.Partition.PartitionId;
                byte[] eventBody = args.Data.EventBody.ToArray();
                //Console.WriteLine($"Partition: {partition}");

                //Console.WriteLine($"Event body: {System.Text.Encoding.Default.GetString(eventBody)}");
                int eventSinceLastCheckpoint = partitionEventCount.AddOrUpdate(
                    key: partition,
                    addValue: 1,
                    updateValueFactory: (_, currentCount) => currentCount + 1);
                // validate checkpoints
                Console.WriteLine($"Events since last checkpoint: {eventSinceLastCheckpoint}");
                if (eventSinceLastCheckpoint >= 50)
                {
                    await args.UpdateCheckpointAsync();
                    partitionEventCount[partition] = 0;
                }
            }
            catch(Exception ex) 
            {
                Console.WriteLine($"Something wrong in HandleEventProcessing {ex.Message}");
            }
        }

        Task HandleEventError(ProcessErrorEventArgs args)
        {
            Console.WriteLine("Error in the EventProcessorClient");
            Console.WriteLine($"\tOperation: {args.Operation}");
            Console.WriteLine($"\tException: {args.Exception}");
            Console.WriteLine("");
            return Task.CompletedTask;
        }

    }
}
