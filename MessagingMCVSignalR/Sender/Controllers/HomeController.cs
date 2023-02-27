using Amazon.SQS;
using Azure.Messaging.ServiceBus;
using Sender.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;

namespace Sender.Controllers
{
    public class HomeController : Controller
    {
        // ====AZURE ATTRIBUTES====

        // connection string to your Service Bus namespace
        static string connectionString = "Endpoint=sb://testingmessaging.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=PGpcLTKk/XIHk4cC+6HD1WalmQPI6/yxf0RxG9McHVE=";

        // name of your Service Bus queue
        static string queueName = "testqueue";

        // the client that owns the connection and can be used to create senders and receivers
        static ServiceBusClient client;

        // the sender used to publish messages to the queue
        static ServiceBusSender sender;

        // ====AWS ATTRIBUTES====

        static string qUrl = "https://sqs.eu-west-2.amazonaws.com/326277667656/testqueue";
        static AmazonSQSClient sqsClient = new AmazonSQSClient();



        public ActionResult Index()
        {
            return RedirectToAction("SendMessage");
        }

        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }

        public ActionResult SendMessage()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Send()
        {
            string messageText = Request.Form.ToString();

            var logMsg = new LogMessage
            {
                Timestamp = DateTime.Now,
                Message = messageText
            };

            string jsonString = JsonSerializer.Serialize(logMsg);

            AzureSendMessage(jsonString);
            AwsSendMessage(jsonString);

            return RedirectToAction("SendMessage");
        }

        static async Task AzureSendMessage(string msg)
        {
            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // Create the clients that we'll use for sending and processing messages.
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(queueName);

            ServiceBusMessage message = new ServiceBusMessage(msg);

            try
            {
                // Use the producer client to send the message to the Service Bus queue
                await sender.SendMessageAsync(message);
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }

        static async Task AwsSendMessage(string msg)
        {
            await sqsClient.SendMessageAsync(qUrl, msg);
        }
    }
}