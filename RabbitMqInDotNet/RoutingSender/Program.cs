using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMqService;

namespace RoutingSender
{
	class Program
	{
		static void Main(string[] args)
		{
			AmqpMessagingService messagingService = new AmqpMessagingService();
			IConnection connection = messagingService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//messagingService.SetUpExchangeAndQueuesForRoutingDemo(model);
			RunRoutingDemo(model, messagingService);
		}

		private static void RunRoutingDemo(IModel model, AmqpMessagingService messagingService)
		{
			Console.WriteLine("Enter your message as follows: the routing key, followed by a semicolon, and then the message. Quit with 'q'.");
			while (true)
			{
				string fullEntry = Console.ReadLine();
				string[] parts = fullEntry.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
				string key = parts[0];
				string message = parts[1];
				if (message.ToLower() == "q") break;
				messagingService.SendRoutingMessage(message, key, model);
			}
		}
	}
}
