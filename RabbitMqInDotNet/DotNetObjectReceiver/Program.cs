using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedObjects;

namespace DotNetObjectReceiver
{
	class Program
	{
		static void Main(string[] args)
		{
			CommonService commonService = new CommonService();
			IConnection connection = commonService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			ReceiveDotNetObjects(model);
		}

		private static void ReceiveDotNetObjects(IModel model)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(CommonService.DotNetObjectQueueName, false, consumer);
			while (true)
			{
				
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				string objectType = deliveryArguments.BasicProperties.Type;
				Type t = Type.GetType(objectType);				
				String jsonified = Encoding.UTF8.GetString(deliveryArguments.Body);
				object rawObject = JsonConvert.DeserializeObject(jsonified, t);
				Console.WriteLine("Object type: {0}", objectType);
								
				if (rawObject.GetType() == typeof(Customer))
				{
					Customer customer = rawObject as Customer;
					Console.WriteLine("Customer name: {0}", customer.Name);
				}
				else if (rawObject.GetType() == typeof(NewCustomer))
				{
					NewCustomer newCustomer = rawObject as NewCustomer;
					Console.WriteLine("NewCustomer name: {0}", newCustomer.Name);
				}
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}
	}
}
