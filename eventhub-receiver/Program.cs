using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using System;
using System.Linq;

namespace eventhub_receiver
{
    class Program
    {
        private static string eventHubsNamespaceConnetionString = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        private static string eventHubName = "eventhub1";
        private static string storageAccountConnectionString = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        private static string containerName = "demo";

        static async Task Main()
        {
            Console.WriteLine("Receving the events from event hub...");

            // Read the default consumer group
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            //Create blob container client that event will use
            BlobContainerClient storageClient = new BlobContainerClient(storageAccountConnectionString, containerName);

            //Create event processor client to process events in event hub

            while (true)
            {
                EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, eventHubsNamespaceConnetionString, eventHubName);

                processor.ProcessEventAsync += ProcessEventHandler;
                processor.ProcessErrorAsync += ProcessErrorHandler;

                // Start the processing

                await processor.StartProcessingAsync();

                // Wait for 10 seconds for the events to be processed
                await Task.Delay(TimeSpan.FromSeconds(10));

                await processor.StopProcessingAsync();

            }
        }

        private static async Task ProcessEventHandler(ProcessEventArgs arg)
        {
            Console.WriteLine(Encoding.UTF8.GetString(arg.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await arg.UpdateCheckpointAsync(arg.CancellationToken);
        }

        private static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
