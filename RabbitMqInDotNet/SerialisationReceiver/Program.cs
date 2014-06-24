using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SharedObjects;

namespace SerialisationReceiver
{
	class Program
	{
		static void Main(string[] args)
		{
			CommonService commonService = new CommonService();
			IConnection connection = commonService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			ReceiveSerialisationMessages(model);
		}

		private static void ReceiveSerialisationMessages(IModel model)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(CommonService.SerialisationQueueName, false, consumer);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				string contentType = deliveryArguments.BasicProperties.ContentType;
				string objectType = deliveryArguments.BasicProperties.Type;
				String jsonified = Encoding.UTF8.GetString(deliveryArguments.Body);
				Customer customer = JsonConvert.DeserializeObject<Customer>(jsonified);
				Console.WriteLine("Pure json: {0}", jsonified);
				Console.WriteLine("Customer name: {0}", customer.Name);
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}

		private Customer DeserialiseFromXml(byte[] messageBody)
		{
			MemoryStream memoryStream = new MemoryStream();
			memoryStream.Write(messageBody, 0, messageBody.Length);
			memoryStream.Seek(0, SeekOrigin.Begin);
			XmlSerializer xmlSerialiser = new XmlSerializer(typeof(Customer));
			return xmlSerialiser.Deserialize(memoryStream) as Customer;
		}

		private Customer DeserialiseFromBinary(byte[] messageBody)
		{
			MemoryStream memoryStream = new MemoryStream();
			memoryStream.Write(messageBody, 0, messageBody.Length);
			memoryStream.Seek(0, SeekOrigin.Begin);
			BinaryFormatter binaryFormatter = new BinaryFormatter();
			return binaryFormatter.Deserialize(memoryStream) as Customer;
		}
	}
}
