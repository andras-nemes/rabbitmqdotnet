using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;

namespace RabbitMqService
{
    public class AmqpMessagingService
    {
		private string _hostName = "localhost";
		private string _userName = "guest";
		private string _password = "guest";
		private string _exchangeName = "";
		private string _oneWayMessageQueueName = "OneWayMessageQueue";
		private string _workerQueueDemoQueueName = "WorkerQueueDemoQueue";
		private string _rpcQueueName = "RpcQueue";
		private string _publishSubscribeExchangeName = "PublishSubscribeExchange";
		private string _publishSubscribeQueueOne = "PublishSubscribeQueueOne";
		private string _publishSubscribeQueueTwo = "PublishSubscribeQueueTwo";
		private string _routingKeyExchange = "RoutingKeyExchange";
		private string _routingKeyQueueOne = "RoutingKeyQueueOne";
		private string _routingKeyQueueTwo = "RoutingKeyQueueTwo";

		private string _topicsExchange = "TopicsExchange";
		private string _topicsQueueOne = "TopicsQueueOne";
		private string _topicsQueueTwo = "TopicsQueueTwo";
		private string _topicsQueueThree = "TopicsQueueThree";

		private string _headersExchange = "HeadersExchange";
		private string _headersQueueOne = "HeadersQueueOne";
		private string _headersQueueTwo = "HeadersQueueTwo";
		private string _headersQueueThree = "HeadersQueueThree";

		private string _scatterGatherExchange = "ScatterGatherExchange";
		private string _scatterGatherReceiverQueueOne = "ScatterGatherReceiverQueueOne";
		private string _scatterGatherReceiverQueueTwo = "ScatterGatherReceiverQueueTwo";
		private string _scatterGatherReceiverQueueThree = "ScatterGatherReceiverQueueThree";

		private bool _durable = true;

		private QueueingBasicConsumer _rpcConsumer;
		private string _responseQueue;

		private QueueingBasicConsumer _scatterGatherConsumer;
		private string _scatterGatherResponseQueue;

		public IConnection GetRabbitMqConnection()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.HostName = _hostName;
			connectionFactory.UserName = _userName;
			connectionFactory.Password = _password;

			return connectionFactory.CreateConnection();
		}

		public void SetUpExchangeAndQueuesForScatterGatherDemo(IModel model)
		{
			model.ExchangeDeclare(_scatterGatherExchange, ExchangeType.Topic, true);
			model.QueueDeclare(_scatterGatherReceiverQueueOne, true, false, false, null);
			model.QueueDeclare(_scatterGatherReceiverQueueTwo, true, false, false, null);
			model.QueueDeclare(_scatterGatherReceiverQueueThree, true, false, false, null);

			model.QueueBind(_scatterGatherReceiverQueueOne, _scatterGatherExchange, "cars");
			model.QueueBind(_scatterGatherReceiverQueueOne, _scatterGatherExchange, "trucks");

			model.QueueBind(_scatterGatherReceiverQueueTwo, _scatterGatherExchange, "cars");
			model.QueueBind(_scatterGatherReceiverQueueTwo, _scatterGatherExchange, "aeroplanes");
			model.QueueBind(_scatterGatherReceiverQueueTwo, _scatterGatherExchange, "buses");

			model.QueueBind(_scatterGatherReceiverQueueThree, _scatterGatherExchange, "cars");
			model.QueueBind(_scatterGatherReceiverQueueThree, _scatterGatherExchange, "buses");
			model.QueueBind(_scatterGatherReceiverQueueThree, _scatterGatherExchange, "tractors");
		}

		public void SetUpExchangeAndQueuesForHeadersDemo(IModel model)
		{
			model.ExchangeDeclare(_headersExchange, ExchangeType.Headers, true);
			model.QueueDeclare(_headersQueueOne, true, false, false, null);
			model.QueueDeclare(_headersQueueTwo, true, false, false, null);
			model.QueueDeclare(_headersQueueThree, true, false, false, null);
			
			Dictionary<string,object> bindingOneHeaders = new Dictionary<string,object>();
			bindingOneHeaders.Add("x-match", "all");
			bindingOneHeaders.Add("category", "animal");
			bindingOneHeaders.Add("type", "mammal");
			model.QueueBind(_headersQueueOne, _headersExchange, "", bindingOneHeaders);

			Dictionary<string, object> bindingTwoHeaders = new Dictionary<string, object>();
			bindingTwoHeaders.Add("x-match", "any");
			bindingTwoHeaders.Add("category", "animal");
			bindingTwoHeaders.Add("type", "insect");
			model.QueueBind(_headersQueueTwo, _headersExchange, "", bindingTwoHeaders);

			Dictionary<string, object> bindingThreeHeaders = new Dictionary<string, object>();
			bindingThreeHeaders.Add("x-match", "any");
			bindingThreeHeaders.Add("category", "plant");
			bindingThreeHeaders.Add("type", "flower");
			model.QueueBind(_headersQueueThree, _headersExchange, "", bindingThreeHeaders);
		}

		public void SetUpExchangeAndQueuesForTopicsDemo(IModel model)
		{
			model.ExchangeDeclare(_topicsExchange, ExchangeType.Topic, true);
			model.QueueDeclare(_topicsQueueOne, true, false, false, null);
			model.QueueDeclare(_topicsQueueTwo, true, false, false, null);
			model.QueueDeclare(_topicsQueueThree, true, false, false, null);
			model.QueueBind(_topicsQueueOne, _topicsExchange, "*.world.*");
			model.QueueBind(_topicsQueueTwo, _topicsExchange, "#.world.#");
			model.QueueBind(_topicsQueueThree, _topicsExchange, "#.world");
		}

		public void SetUpExchangeAndQueuesForRoutingDemo(IModel model)
		{
			model.ExchangeDeclare(_routingKeyExchange, ExchangeType.Direct, true);
			model.QueueDeclare(_routingKeyQueueOne, true, false, false, null);
			model.QueueDeclare(_routingKeyQueueTwo, true, false, false, null);
			model.QueueBind(_routingKeyQueueOne, _routingKeyExchange, "cars");
			model.QueueBind(_routingKeyQueueTwo, _routingKeyExchange, "trucks");
		}

		public void SetUpExchangeAndQueuesForDemo(IModel model)
		{
			model.ExchangeDeclare(_publishSubscribeExchangeName, ExchangeType.Fanout, true);
			model.QueueDeclare(_publishSubscribeQueueOne, true, false, false, null);
			model.QueueDeclare(_publishSubscribeQueueTwo, true, false, false, null);
			model.QueueBind(_publishSubscribeQueueOne, _publishSubscribeExchangeName, "");
			model.QueueBind(_publishSubscribeQueueTwo, _publishSubscribeExchangeName, "");
		}

		public void SetUpQueueForRpcDemo(IModel model)
		{
			model.QueueDeclare(_rpcQueueName, _durable, false, false, null);
		}

		public void SetUpQueueForWorkerQueueDemo(IModel model)
		{
			model.QueueDeclare(_workerQueueDemoQueueName, _durable, false, false, null);
		}

		public void SetUpQueueForOneWayMessageDemo(IModel model)
		{
			model.QueueDeclare(_oneWayMessageQueueName, _durable, false, false, null);
		}

		public void SendMessageToPublishSubscribeQueues(string message, IModel model)
		{
			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.SetPersistent(_durable);
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish(_publishSubscribeExchangeName, "", basicProperties, messageBytes);
		}

		public List<string> SendScatterGatherMessageToQueues(string message, IModel model, TimeSpan timeout, string routingKey
			, int minResponses)
		{
			List<string> responses = new List<string>();
			if (string.IsNullOrEmpty(_scatterGatherResponseQueue))
			{
				_scatterGatherResponseQueue = model.QueueDeclare().QueueName;
			}

			if (_scatterGatherConsumer == null)
			{
				_scatterGatherConsumer = new QueueingBasicConsumer(model);
				model.BasicConsume(_scatterGatherResponseQueue, true, _scatterGatherConsumer);
			}

			string correlationId = Guid.NewGuid().ToString();
			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.ReplyTo = _scatterGatherResponseQueue;
			basicProperties.CorrelationId = correlationId;

			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish(_scatterGatherExchange, routingKey, basicProperties, messageBytes);
			
			DateTime timeoutDate = DateTime.UtcNow + timeout;
			while (DateTime.UtcNow <= timeoutDate)
			{
				BasicDeliverEventArgs deliveryArguments;
				_scatterGatherConsumer.Queue.Dequeue(500, out deliveryArguments);
				if (deliveryArguments != null && deliveryArguments.BasicProperties != null
					&& deliveryArguments.BasicProperties.CorrelationId == correlationId)
				{
					string response = Encoding.UTF8.GetString(deliveryArguments.Body);
					responses.Add(response);
					if (responses.Count >= minResponses)
					{
						break;
					}
				}
			}

			return responses;
		}

		public string SendRpcMessageToQueue(string message, IModel model, TimeSpan timeout)
		{
			if (string.IsNullOrEmpty(_responseQueue))
			{
				_responseQueue = model.QueueDeclare().QueueName;
			}

			if (_rpcConsumer == null)
			{
				_rpcConsumer = new QueueingBasicConsumer(model);
				model.BasicConsume(_responseQueue, true, _rpcConsumer);
			}

			string correlationId = Guid.NewGuid().ToString();

			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.ReplyTo = _responseQueue;
			basicProperties.CorrelationId = correlationId;

			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish("", _rpcQueueName, basicProperties, messageBytes);

			DateTime timeoutDate = DateTime.UtcNow + timeout;
			while (DateTime.UtcNow <= timeoutDate)
			{
				BasicDeliverEventArgs deliveryArguments = (BasicDeliverEventArgs)_rpcConsumer.Queue.Dequeue();
				if (deliveryArguments.BasicProperties != null
					&& deliveryArguments.BasicProperties.CorrelationId == correlationId)
				{
					string response = Encoding.UTF8.GetString(deliveryArguments.Body);
					return response;
				}
			}
			throw new TimeoutException("No response before the timeout period.");
		}

		public void SendMessageToWorkerQueue(string message, IModel model)
		{
			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.SetPersistent(_durable);
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish(_exchangeName, _workerQueueDemoQueueName, basicProperties, messageBytes);
		}

		public void SendHeadersMessage(string message, Dictionary<string,object> headers, IModel model)
		{
			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.SetPersistent(_durable);
			basicProperties.Headers = headers;
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish(_headersExchange, "", basicProperties, messageBytes);
		}

		public void SendOneWayMessage(string message, IModel model)
		{
			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.SetPersistent(_durable);
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish(_exchangeName, _oneWayMessageQueueName, basicProperties, messageBytes);
		}

		public void SendTopicsMessage(string message, string routingKey, IModel model)
		{
			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.SetPersistent(_durable);
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish(_topicsExchange, routingKey, basicProperties, messageBytes);
		}

		public void SendRoutingMessage(string message, string routingKey, IModel model)
		{
			IBasicProperties basicProperties = model.CreateBasicProperties();
			basicProperties.SetPersistent(_durable);
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			model.BasicPublish(_routingKeyExchange, routingKey, basicProperties, messageBytes);
		}

		public void ReceiveRpcMessage(IModel model)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(_rpcQueueName, false, consumer);

			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				string message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message: {0} ; {1}", message, " Enter your response: ");
				string response = Console.ReadLine();
				IBasicProperties replyBasicProperties = model.CreateBasicProperties();
				replyBasicProperties.CorrelationId = deliveryArguments.BasicProperties.CorrelationId;
				byte[] responseBytes = Encoding.UTF8.GetBytes(response);
				model.BasicPublish("", deliveryArguments.BasicProperties.ReplyTo, replyBasicProperties, responseBytes);
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}

		public void ReceiveScatterGatherMessageOne(IModel model)
		{
			ReceiveScatterGatherMessage(model, _scatterGatherReceiverQueueOne);
		}

		public void ReceiveScatterGatherMessageTwo(IModel model)
		{
			ReceiveScatterGatherMessage(model, _scatterGatherReceiverQueueTwo);
		}

		public void ReceiveScatterGatherMessageThree(IModel model)
		{
			ReceiveScatterGatherMessage(model, _scatterGatherReceiverQueueThree);
		}

		private void ReceiveScatterGatherMessage(IModel model, string queueName)
		{
			model.BasicQos(0, 1, false);
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(queueName, false, consumer);

			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				string message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message: {0} ; {1}", message, " Enter your response: ");
				string response = Console.ReadLine();
				IBasicProperties replyBasicProperties = model.CreateBasicProperties();
				replyBasicProperties.CorrelationId = deliveryArguments.BasicProperties.CorrelationId;
				byte[] responseBytes = Encoding.UTF8.GetBytes(response);
				model.BasicPublish("", deliveryArguments.BasicProperties.ReplyTo, replyBasicProperties, responseBytes);
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}

		public void ReceiveHeadersMessageReceiverOne(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _headersQueueOne, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				StringBuilder messageBuilder = new StringBuilder();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				messageBuilder.Append("Message from queue: ").Append(message).Append(". ");
				foreach (string headerKey in deliveryArguments.BasicProperties.Headers.Keys)
				{
					byte[] value = deliveryArguments.BasicProperties.Headers[headerKey] as byte[];
					messageBuilder.Append("Header key: ").Append(headerKey).Append(", value: ").Append(Encoding.UTF8.GetString(value)).Append("; ");
				}
				
				Console.WriteLine(messageBuilder.ToString());
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveHeadersMessageReceiverTwo(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _headersQueueTwo, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				StringBuilder messageBuilder = new StringBuilder();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				messageBuilder.Append("Message from queue: ").Append(message).Append(". ");
				foreach (string headerKey in deliveryArguments.BasicProperties.Headers.Keys)
				{
					byte[] value = deliveryArguments.BasicProperties.Headers[headerKey] as byte[];
					messageBuilder.Append("Header key: ").Append(headerKey).Append(", value: ").Append(Encoding.UTF8.GetString(value)).Append("; ");
				}

				Console.WriteLine(messageBuilder.ToString());
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveHeadersMessageReceiverThree(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _headersQueueThree, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				StringBuilder messageBuilder = new StringBuilder();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				messageBuilder.Append("Message from queue: ").Append(message).Append(". ");
				foreach (string headerKey in deliveryArguments.BasicProperties.Headers.Keys)
				{
					byte[] value = deliveryArguments.BasicProperties.Headers[headerKey] as byte[];
					messageBuilder.Append("Header key: ").Append(headerKey).Append(", value: ").Append(Encoding.UTF8.GetString(value)).Append("; ");
				}

				Console.WriteLine(messageBuilder.ToString());
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveRoutingMessageReceiverOne(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _routingKeyQueueOne, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveRoutingMessageReceiverTwo(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _routingKeyQueueTwo, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveTopicMessageReceiverOne(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _topicsQueueOne, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveTopicMessageReceiverTwo(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _topicsQueueTwo, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveTopicMessageReceiverThree(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _topicsQueueThree, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceivePublishSubscribeMessageReceiverOne(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _publishSubscribeQueueOne, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceivePublishSubscribeMessageReceiverTwo(IModel model)
		{
			model.BasicQos(0, 1, false);
			Subscription subscription = new Subscription(model, _publishSubscribeQueueTwo, false);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = subscription.Next();
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message from queue: {0}", message);
				subscription.Ack(deliveryArguments);
			}
		}

		public void ReceiveWorkerQueueMessages(IModel model)
		{
			model.BasicQos(0, 1, false); //basic quality of service
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(_workerQueueDemoQueueName, false, consumer);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message received: {0}", message);
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}

		public void ReceiveOneWayMessages(IModel model)
		{
			model.BasicQos(0, 1, false); //basic quality of service
			QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
			model.BasicConsume(_oneWayMessageQueueName, false, consumer);
			while (true)
			{
				BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
				String message = Encoding.UTF8.GetString(deliveryArguments.Body);
				Console.WriteLine("Message received: {0}", message);
				model.BasicAck(deliveryArguments.DeliveryTag, false);
			}
		}
    }
}
