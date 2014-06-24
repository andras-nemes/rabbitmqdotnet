using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMqService;

namespace HeadersSender
{
	class Program
	{
		static void Main(string[] args)
		{
			AmqpMessagingService messagingService = new AmqpMessagingService();
			IConnection connection = messagingService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//messagingService.SetUpExchangeAndQueuesForHeadersDemo(model);
			RunHeadersDemo(model, messagingService);
		}

		private static void RunHeadersDemo(IModel model, AmqpMessagingService messagingService)
		{
			Console.WriteLine("Enter your message as follows: the header values for 'category' and 'type separated by a colon. Then put a semicolon, and then the message. Quit with 'q'.");
			while (true)
			{
				string fullEntry = Console.ReadLine();
				string[] parts = fullEntry.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
				string headers = parts[0];
				string[] headerValues = headers.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
				Dictionary<string, object> headersDictionary = new Dictionary<string, object>();
				headersDictionary.Add("category", headerValues[0]);
				headersDictionary.Add("type", headerValues[1]);
				string message = parts[1];
				if (message.ToLower() == "q") break;
				messagingService.SendHeadersMessage(message, headersDictionary, model);
			}
		}
	}
}
