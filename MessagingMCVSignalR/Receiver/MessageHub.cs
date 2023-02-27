using Amazon.SQS;
using Amazon.SQS.Model;
using Azure.Messaging.ServiceBus;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Hubs;
using Receiver.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Web;

namespace Receiver
{
    public class MessageHub : Hub
    {
        // ====AZURE====

        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://testingmessaging.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=PGpcLTKk/XIHk4cC+6HD1WalmQPI6/yxf0RxG9McHVE=";

        // name of your Service Bus queue
        static string queueName = "testqueue";

        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the processor that reads and processes messages from the queue
        static ServiceBusProcessor processor;


        // handle received messages
        async Task AzureMessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            LogMessage logMsg = JsonSerializer.Deserialize<LogMessage>(body);
            Send(logMsg.Timestamp.ToString(), logMsg.Message, 1);
            

            // complete the message. message is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        Task AzureErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        async Task AzureProcessMessages()
        {
            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //

            // Create the client object that will be used to create sender and receiver objects
            client = new ServiceBusClient(connectionString);

            // create a processor that we can use to process the messages
            processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());

            // add handler to process messages
            processor.ProcessMessageAsync += AzureMessageHandler;

            // add handler to process any errors
            processor.ProcessErrorAsync += AzureErrorHandler;

            // start processing 
            await processor.StartProcessingAsync();
        }


        // ====AWS====

        static string qUrl = "https://sqs.eu-west-2.amazonaws.com/326277667656/testqueue";


        private async Task<ReceiveMessageResponse> GetMessage(IAmazonSQS sqsClient, string qUrl, int waitTime = 0)
        {
            return await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = qUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = waitTime
            });
        }

        private bool ProcessMessage(Message message)
        {
            string body = message.Body.ToString();
            LogMessage logMsg = JsonSerializer.Deserialize<LogMessage>(body);
            Send(logMsg.Timestamp.ToString(), logMsg.Message, 2);
            return true;
        }

        private async Task DeleteMessage(
          IAmazonSQS sqsClient, Message message, string qUrl)
        {
            await sqsClient.DeleteMessageAsync(qUrl, message.ReceiptHandle);
        }

        private async Task AwsProcessMessages()
        {
            var sqsClient = new AmazonSQSClient();

            do
            {
                var msg = await GetMessage(sqsClient, qUrl, 2);
                if (msg.Messages.Count != 0)
                {
                    if (ProcessMessage(msg.Messages[0]))
                        await DeleteMessage(sqsClient, msg.Messages[0], qUrl);
                }
            } while (true);
        }




        [HubMethodName("StartProcessing")]
        public void StartProcessing()
        {
            AzureProcessMessages();
            AwsProcessMessages();
        }

        public void Send(string name, string message, int service)
        {
            // Call the addNewMessageToPage method to update clients.
            Clients.All.addNewMessageToPage(name, message, service);
        }
    }
}