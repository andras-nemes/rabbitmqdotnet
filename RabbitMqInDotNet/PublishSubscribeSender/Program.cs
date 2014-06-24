using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMqService;

namespace PublishSubscribeSender
{
	class Program
	{
		static void Main(string[] args)
		{
			AmqpMessagingService messagingService = new AmqpMessagingService();
			IConnection connection = messagingService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//messagingService.SetUpExchangeAndQueuesForDemo(model);
			RunPublishSubscribeMessageDemo(model, messagingService);

		}

		private static void RunPublishSubscribeMessageDemo(IModel model, AmqpMessagingService messagingService)
		{
			Console.WriteLine("Enter your message and press Enter. Quit with 'q'.");
			while (true)
			{
				string message = Console.ReadLine();
				if (message.ToLower() == "q") break;

				messagingService.SendMessageToPublishSubscribeQueues(message, model);
			}
		}
	}
}
