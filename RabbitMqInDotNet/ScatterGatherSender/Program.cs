using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMqService;

namespace ScatterGatherSender
{
	class Program
	{
		static void Main(string[] args)
		{
			AmqpMessagingService messagingService = new AmqpMessagingService();
			IConnection connection = messagingService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//messagingService.SetUpExchangeAndQueuesForScatterGatherDemo(model);
			RunScatterGatherDemo(model, messagingService);
		}

		private static void RunScatterGatherDemo(IModel model, AmqpMessagingService messagingService)
		{
			Console.WriteLine("Enter your message as follows: the routing key, followed by a semicolon, and then the message. Quit with 'q'.");
			while (true)
			{
				string fullEntry = Console.ReadLine();
				string[] parts = fullEntry.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
				string key = parts[0];
				string message = parts[1];
				if (message.ToLower() == "q") break;
				//method needs model, routing key, timeout, message
				List<string> responses = messagingService.SendScatterGatherMessageToQueues(message, model, TimeSpan.FromSeconds(20), key, 3);
				Console.WriteLine("Received the following messages: ");
				foreach (string response in responses)
				{
					Console.WriteLine(response);
				}
			}
		}
	}
}
