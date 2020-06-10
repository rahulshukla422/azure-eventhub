using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace eventhub_sender
{
    class Program
    {
        private static string eventhubnamespaceConnectionString = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        private static string eventhubname = "eventhub1";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Sending the events to event hub...");

            // Create a producer client that you can use to send events to an event hub
            await using (EventHubProducerClient client = new EventHubProducerClient(eventhubnamespaceConnectionString, eventhubname))
            {
                // Create a batch of events 

                for (int i = 0; i < 100; i++)
                {
                    EventDataBatch eventDataBatch = await client.CreateBatchAsync();

                    Console.WriteLine($"event{i}");
                    // Add events to the batch. An event is a represented by a collection of bytes and metadata. 
                    eventDataBatch.TryAdd(new Azure.Messaging.EventHubs.EventData(Encoding.UTF8.GetBytes($"event{i}")));

                    await client.SendAsync(eventDataBatch);

                    Thread.Sleep(1000);
                }

            }


            Console.ReadLine();
        }
    }
}
