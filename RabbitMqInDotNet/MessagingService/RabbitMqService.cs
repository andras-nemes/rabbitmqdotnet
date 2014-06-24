using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace MessagingService
{
	public class RabbitMqService
	{
		private string _hostName = "localhost";
		private string _userName = "guest";
		private string _password = "guest";

		public static string LargeMessageBufferedQueue = "LargeMessageBufferedQueue";
		public static string ChunkedMessageBufferedQueue = "ChunkedMessageBufferedQueue";

		public IConnection GetRabbitMqConnection()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.HostName = _hostName;
			connectionFactory.UserName = _userName;
			connectionFactory.Password = _password;

			return connectionFactory.CreateConnection();
		}
	}
}
