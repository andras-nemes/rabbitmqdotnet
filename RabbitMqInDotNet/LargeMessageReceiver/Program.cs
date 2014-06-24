using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MessagingService;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace LargeMessageReceiver
{
	class Program
	{
		static void Main(string[] args)
		{
			RabbitMqService commonService = new RabbitMqService();
			IConnection connection = commonService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//ReceiveBufferedMessages(model);
			ReceiveChunkedMessages(model);
		}

		private static void ReceiveChunkedMessages(IModel model)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(RabbitMqService.ChunkedMessageBufferedQueue, false, consumer);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				Console.WriteLine("Received a chunk!");
				IDictionary<string, object> headers = deliveryArguments.BasicProperties.Headers;
				string randomFileName = Encoding.UTF8.GetString((headers["output-file"] as byte[]));
				bool isLastChunk = Convert.ToBoolean(headers["finished"]);
				string localFileName = string.Concat(@"c:\", randomFileName);
				using (FileStream fileStream = new FileStream(localFileName, FileMode.Append, FileAccess.Write))
				{
					fileStream.Write(deliveryArguments.Body, 0, deliveryArguments.Body.Length);
					fileStream.Flush();
				}
				Console.WriteLine("Chunk saved. Finished? {0}", isLastChunk);
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}

		private static void ReceiveBufferedMessages(IModel model)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(RabbitMqService.LargeMessageBufferedQueue, false, consumer);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				byte[] messageContents = deliveryArguments.Body;
				string randomFileName = string.Concat(@"c:\large_file_from_rabbit_", Guid.NewGuid(), ".txt");
				Console.WriteLine("Received message, will save it to {0}", randomFileName);
				File.WriteAllBytes(randomFileName, messageContents);
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}
	}
}
