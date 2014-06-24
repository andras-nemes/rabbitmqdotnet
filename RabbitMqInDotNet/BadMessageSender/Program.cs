using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MessageService;
using RabbitMQ.Client;

namespace BadMessageSender
{
	class Program
	{
		static void Main(string[] args)
		{
			RabbitMqService rabbitMqService = new RabbitMqService();
			IConnection connection = rabbitMqService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//model.QueueDeclare(RabbitMqService.BadMessageBufferedQueue, true, false, false, null);
			RunBadMessageDemo(model);
		}

		private static void RunBadMessageDemo(IModel model)
		{
			Console.WriteLine("Enter your message. Quit with 'q'.");
			while (true)
			{
				string message = Console.ReadLine();
				if (message.ToLower() == "q") break;
				IBasicProperties basicProperties = model.CreateBasicProperties();
				basicProperties.SetPersistent(true);
				byte[] messageBuffer = Encoding.UTF8.GetBytes(message);
				model.BasicPublish("", RabbitMqService.BadMessageBufferedQueue, basicProperties, messageBuffer);
			}
		}
	}
}
