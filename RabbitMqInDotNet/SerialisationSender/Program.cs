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
using SharedObjects;

namespace SerialisationSender
{
	class Program
	{
		static void Main(string[] args)
		{
			CommonService commonService = new CommonService();
			IConnection connection = commonService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			string fullyQualifiedName = typeof(Customer).ToString();
			//SetupSerialisationMessageQueue(model);
			RunSerialisationDemo(model);
		}

		private static void SetupSerialisationMessageQueue(IModel model)
		{
			model.QueueDeclare(CommonService.SerialisationQueueName, true, false, false, null);
		}

		private static void RunSerialisationDemo(IModel model)
		{
			Console.WriteLine("Enter customer name. Quit with 'q'.");
			while (true)
			{
				string customerName = Console.ReadLine();
				if (customerName.ToLower() == "q") break;
				Customer customer = new Customer() { Name = customerName };
				IBasicProperties basicProperties = model.CreateBasicProperties();
				basicProperties.SetPersistent(true);
				basicProperties.ContentType = "application/json";
				basicProperties.Type = "Customer";
				String jsonified = JsonConvert.SerializeObject(customer);
				byte[] customerBuffer = Encoding.UTF8.GetBytes(jsonified);
				model.BasicPublish("", CommonService.SerialisationQueueName, basicProperties, customerBuffer);
			}
		}

		private static byte[] SerialiseIntoXml(Customer customer)
		{
			MemoryStream memoryStream = new MemoryStream();
			XmlSerializer xmlSerialiser = new XmlSerializer(customer.GetType());
			xmlSerialiser.Serialize(memoryStream, customer);
			memoryStream.Flush();
			memoryStream.Seek(0, SeekOrigin.Begin);
			return memoryStream.GetBuffer();
		}

		private static byte[] SerialiseIntoBinary(Customer customer)
		{
			MemoryStream memoryStream = new MemoryStream();
			BinaryFormatter binaryFormatter = new BinaryFormatter();
			binaryFormatter.Serialize(memoryStream, customer);
			memoryStream.Flush();
			memoryStream.Seek(0, SeekOrigin.Begin);
			return memoryStream.GetBuffer();
		}
	}
}
