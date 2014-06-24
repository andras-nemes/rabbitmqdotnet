using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using SharedObjects;

namespace DotNetObjectSender
{
	class Program
	{
		static void Main(string[] args)
		{
			CommonService commonService = new CommonService();
			IConnection connection = commonService.GetRabbitMqConnection();
			IModel model = connection.CreateModel();
			//model.QueueDeclare(CommonService.DotNetObjectQueueName, true, false, false, null);
			RunDotNetObjectDemo(model);
		}

		private static void RunDotNetObjectDemo(IModel model)
		{
			Console.WriteLine("Enter customer name. Quit with 'q'.");
			while (true)
			{
				string customerName = Console.ReadLine();
				if (customerName.ToLower() == "q") break;
				Random random = new Random();
				int i = random.Next(0, 2);
				String type = "";
				String jsonified = "";
				if (i == 0)
				{
					Customer customer = new Customer() { Name = customerName };
					jsonified = JsonConvert.SerializeObject(customer);
					type = customer.GetType().AssemblyQualifiedName;
				}
				else
				{
					NewCustomer newCustomer = new NewCustomer() { Name = customerName };
					jsonified = JsonConvert.SerializeObject(newCustomer);
					type = newCustomer.GetType().AssemblyQualifiedName;
				}
				
				IBasicProperties basicProperties = model.CreateBasicProperties();
				basicProperties.SetPersistent(true);
				basicProperties.ContentType = "application/json";
				basicProperties.Type = type;
				byte[] customerBuffer = Encoding.UTF8.GetBytes(jsonified);
				model.BasicPublish("", CommonService.DotNetObjectQueueName, basicProperties, customerBuffer);
			}
		}
	}
}
