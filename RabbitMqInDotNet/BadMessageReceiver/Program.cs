using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MessageService;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace BadMessageReceiver
{
	class Program
	{
		static void Main(string[] args)
		{
			RabbitMqService messageService = new RabbitMqService();
			IConnection connection = messageService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//ReceiveBadMessages(model);
			ReceiveBadMessageExtended(model);
		}

		private static void ReceiveBadMessages(IModel model)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(RabbitMqService.BadMessageBufferedQueue, false, consumer);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				Random random = new Random();
				int i = random.Next(0, 2);

				//pretend that message cannot be processed and must be rejected
				if (i == 1) //reject the message and discard completely
				{
					Console.WriteLine("Rejecting and discarding message {0}", message);
					model.BasicReject(deliveryArguments.DeliveryTag, false);
				}
				else //reject the message but push back to queue for later re-try
				{
					Console.WriteLine("Rejecting message and putting it back to the queue: {0}", message);
					model.BasicReject(deliveryArguments.DeliveryTag, true);
				}
			}
		}

		private static void ReceiveBadMessageExtended(IModel model)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(RabbitMqService.BadMessageBufferedQueue, false, consumer);
			string customRetryHeaderName = "number-of-retries";
			int maxNumberOfRetries = 3;
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				Random random = new Random();
				int i = random.Next(0, 3);
				int retryCount = GetRetryCount(deliveryArguments.BasicProperties, customRetryHeaderName);
				if (i == 2) //no exception, accept message
				{
					Console.WriteLine("Message {0} accepted. Number of retries: {1}", message, retryCount);
					model.BasicAck(deliveryArguments.DeliveryTag, false);
				}
				else //simulate exception: accept message, but create copy and throw back
				{
					if (retryCount < maxNumberOfRetries)
					{
						Console.WriteLine("Message {0} has thrown an exception. Current number of retries: {1}", message, retryCount);
						IBasicProperties propertiesForCopy = model.CreateBasicProperties();
						IDictionary<string, object> headersCopy = CopyHeaders(deliveryArguments.BasicProperties);
						propertiesForCopy.Headers = headersCopy;
						propertiesForCopy.Headers[customRetryHeaderName] = ++retryCount;
						model.BasicPublish(deliveryArguments.Exchange, deliveryArguments.RoutingKey, propertiesForCopy, deliveryArguments.Body);
						model.BasicAck(deliveryArguments.DeliveryTag, false);
						Console.WriteLine("Message {0} thrown back at queue for retry. New retry count: {1}", message, retryCount);
					}
					else //must be rejected, cannot process
					{
						Console.WriteLine("Message {0} has reached the max number of retries. It will be rejected.", message);
						model.BasicReject(deliveryArguments.DeliveryTag, false);
					}
				}
			}
		}

		private static IDictionary<string, object> CopyHeaders(IBasicProperties originalProperties)
		{
			IDictionary<string, object> dict = new Dictionary<string, object>();
			IDictionary<string, object> headers = originalProperties.Headers;
			if (headers != null)
			{
				foreach (KeyValuePair<string, object> kvp in headers)
				{
					dict[kvp.Key] = kvp.Value;
				}
			}

			return dict;
		}

		private static int GetRetryCount(IBasicProperties messageProperties, string countHeader)
		{
			IDictionary<string, object> headers = messageProperties.Headers;
			int count = 0;
			if (headers != null)
			{
				if (headers.ContainsKey(countHeader))
				{
					string countAsString = Convert.ToString( headers[countHeader]);
					count = Convert.ToInt32(countAsString);
				}
			}

			return count;
		}
	}
}
