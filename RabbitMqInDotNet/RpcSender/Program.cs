using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMqService;

namespace RpcSender
{
	class Program
	{
		static void Main(string[] args)
		{
			AmqpMessagingService messagingService = new AmqpMessagingService();
			IConnection connection = messagingService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//messagingService.SetUpQueueForRpcDemo(model);
			RunRpcMessageDemo(model, messagingService);
		}

		private static void RunRpcMessageDemo(IModel model, AmqpMessagingService messagingService)
		{
			Console.WriteLine("Enter your message and press Enter. Quit with 'q'.");
			while (true)
			{
				string message = Console.ReadLine();
				if (message.ToLower() == "q") break;

				String response = messagingService.SendRpcMessageToQueue(message, model, TimeSpan.FromMinutes(1));
				Console.WriteLine("Response: {0}", response);
			}
		}
	}
}
