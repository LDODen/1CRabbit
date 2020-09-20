using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Runtime;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace SR
{
    public interface ASignatures
    {

        string Connect();
        string DisConnect();
        string SendMessageWC(string message);
        string SendMessage(string message);
        string SendMessage(string HostName, int Port, string UserName, string Password, string Exchange, string RoutingKey, string message);
        string ReceiveMessageWC(string queuename);
        string ReceiveMessage(string queuename);
        string MessagesInQueue(string queuename);
        string Ack(string queuename);
        string Nack(string queuename);

    }

    [Guid("AB634001-F13D-11d0-A459-004095E1DAEA")]// стандартный GUID для IInitDone ссылка http://soaron.fromru.com/vkhints.htm
    [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    public interface IInitDone
    {
        /// <summary>
        /// Инициализация компонента
        /// </summary>
        /// <param name="connection">reference to IDispatch</param>
        void Init([MarshalAs(UnmanagedType.IDispatch)] object connection);

        /// <summary>
        /// Вызывается перед уничтожением компонента
        /// </summary>
        void Done();

        /// <summary>
        /// Возвращается инициализационная информация
        /// </summary>
        /// <param name="info">Component information</param>
        void GetInfo([MarshalAs(UnmanagedType.SafeArray, SafeArraySubType = VarEnum.VT_VARIANT)] ref object[] info);
    }

    [ClassInterface(ClassInterfaceType.AutoDual)]

    //public class ArsClass : ASignatures, IInitDone
    public class OneSRabbit : ASignatures, IInitDone
    {

        /// <summary>
        /// Инициализация компонента
        /// </summary>
        /// <param name="connection">reference to IDispatch</param>
        public void Init([MarshalAs(UnmanagedType.IDispatch)] object connection)
        {
            //asyncEvent = (IAsyncEvent)connection;
            //statusLine = (IStatusLine)connection;
        }

        /// <summary>
        /// Возвращается информация о компоненте
        /// </summary>
        /// <param name="info">Component information</param>
        public void GetInfo([MarshalAs(UnmanagedType.SafeArray, SafeArraySubType = VarEnum.VT_VARIANT)] ref object[] info)
        {
            info[0] = 2000;
        }

        public void Done()
        {
        }

        const bool autoAck = false;
        public string UserName { get; set; }
        public string Password { get; set; }
        public string HostName { get; set; }
        public int Port { get; set; }

        public string Exchange { get; set; }
        public string RoutingKey { get; set; }

        private ConnectionFactory fact = null;
        private IConnection conn = null;
        private IModel chan = null;

        private IBasicProperties props = null;


        //CONNECT
        public string Connect()
        {
            if (fact == null)
            {
                try
                {
                    fact = new ConnectionFactory()
                    {
                        HostName = HostName,
                        UserName = UserName,
                        Password = Password,
                        Port = Port

                    };

                    fact.RequestedHeartbeat = 10;
                    fact.RequestedConnectionTimeout = 5000;

                    conn = fact.CreateConnection();

                    chan = conn.CreateModel();

                    props = chan.CreateBasicProperties();
                    props.DeliveryMode = 2;
                    props.Persistent = true;

                    return "Connection established!";
                }
                catch (Exception e)
                {
                    return e.ToString();
                }
            }
            else
            {
                return "Connection already established!";
            }
        }

        //DISCONNECT
        public string DisConnect()
        {
            if (fact != null)
            {
                chan.Close();
                conn.Close();
                chan = null;
                conn = null;
                fact = null;
                props = null;
                return "Connection closed!";
            }
            else
            {
                return "No active connection!";
            }
        }


        //SENDING
        public string SendMessageWC(string message)
        {
            if (fact != null)
            {
                try
                {
                    Byte[] body = Encoding.UTF8.GetBytes(message);
                    chan.BasicPublish(Exchange, RoutingKey, props, body);
                }
                catch (Exception e)
                {
                    return e.ToString();
                }

                return "OK!";
            }
            else
            {
                return "No active connection!";
            }

        }

        public string SendMessage(string message)
        {

            try
            {
                ConnectionFactory factory = new ConnectionFactory()
                {
                    HostName = HostName,
                    UserName = UserName,
                    Password = Password,
                    Port = Port
                };
                factory.RequestedHeartbeat = 10;
                factory.RequestedConnectionTimeout = 5000;

                using (IConnection connection = factory.CreateConnection())
                {
                    using (IModel chanell = connection.CreateModel())
                    {
                        props = chanell.CreateBasicProperties();
                        props.DeliveryMode = 2;
                        props.Persistent = true;
                        Byte[] body = Encoding.UTF8.GetBytes(message);
                        chanell.BasicPublish(Exchange, RoutingKey, props, body);

                    }
                }
            }
            catch (Exception e)
            {
                return e.ToString();
            }

            return "OK!";
        }

        public string SendMessage(string lHostName, int lPort, string lUserName, string lPassword, string lExchange, string lRoutingKey, string lmessage)
        {
            try
            {
                ConnectionFactory lfactory = new ConnectionFactory()
                {
                    HostName = lHostName,
                    UserName = lUserName,
                    Password = lPassword,
                    Port = lPort
                };
                lfactory.RequestedHeartbeat = 10;
                lfactory.RequestedConnectionTimeout = 5000;

                using (IConnection lconnection = lfactory.CreateConnection())
                {
                    using (IModel lchanell = lconnection.CreateModel())
                    {
                        props = lchanell.CreateBasicProperties();
                        props.DeliveryMode = 2;
                        props.Persistent = true;
                        Byte[] lbody = Encoding.UTF8.GetBytes(lmessage);
                        lchanell.BasicPublish(lExchange, lRoutingKey, props, lbody);

                    }
                }
            }
            catch (Exception e)
            {
                return e.ToString();
            }

            return "OK!";

        }

        //RECEIVING
        public string ReceiveMessageWC(string queuename)
        {
            try
            {
                BasicGetResult data = chan.BasicGet(queuename, false);
                // BasicConsume(queuename, autoAck, consumer);

                //var message = System.Text.Encoding.UTF8.GetString(data.Body);
                string message = System.Text.Encoding.UTF8.GetString(data.Body);

                chan.BasicAck(data.DeliveryTag, false);

                return message;
            }
            catch (Exception e)
            {
                return e.ToString();
            }
        } //ReceiveMessageWC

        public string ReceiveMessage(string queuename)
        {
            try
            {
                BasicGetResult data = chan.BasicGet(queuename, false);

                string body = System.Text.Encoding.UTF8.GetString(data.Body);
                string routing_key = data.RoutingKey;
                string delivery_tag = data.DeliveryTag.ToString();

                string message = "{'#type': 'jv8:Structure', '#value': [{ 'name': {'#type': 'jxs:string', '#value': 'routing_key'},'Value': {" +
                                    "'#type': 'jxs:string',	'#value': '" + routing_key + "' }}," +
                                "{'name': {'#type': 'jxs:string','#value': 'delivery_tag'},	'Value': {'#type': 'jxs:string', '#value': '" + delivery_tag + "' }}," +
                                "{'name': {'#type': 'jxs:string','#value': 'body'}, 'Value': {'#type': 'jxs:string', '#value': '" + body + "' }}]}";

                chan.BasicAck(data.DeliveryTag, false);

                try
                {
                    message = EscapeSymbols(message);
                }
                catch { }

                return message;
            }
            catch (Exception e)
            {
                return e.ToString();
            }
        }//ReceiveMessage

        private string EscapeSymbols(string input)
        {
            string st = input.Replace("\r\n", "").Replace("\t", "").Replace("},", "},\r\n");
            string pattern = "value\": \"(.*?)\"}";

            MatchCollection matches = Regex.Matches(st, pattern);

            string output = input;
            string new_val;
            foreach (Match match in matches)
            {
                string val = match.Groups[1].Value;
                string old_st = "\"";
                string new_st = @"\" + "\"";
                if ((val.IndexOf("lue", 0) == -1) && (val != ""))
                {
                    new_val = val.Replace(@"\", @"\\");
                    output = output.Replace(val, new_val.Replace(old_st, new_st));
                    //output = output.Replace(val, val.Replace(old_st, new_st));
                }
            }

            return output;
        }


        public string AckWC(string queuename)
        {
            try
            {
                IConnection connection = this.conn;
                IModel chanell = this.chan;
                using (connection)
                {
                    using (chanell)
                    {

                        chanell.QueueDeclare(queue: queuename,
                                          durable: true,
                                          exclusive: false,
                                          autoDelete: false,
                                          arguments: null);

                        QueueingBasicConsumer consumer = new QueueingBasicConsumer(chanell);

                        chanell.BasicConsume(queuename, autoAck, consumer);
                        BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                        chanell.BasicAck(ea.DeliveryTag, false);
                        return "OK!";
                    }
                }
            }
            catch (Exception e)
            {
                return e.ToString();
            }
        }//AckWС

        //
        public string Ack(string queuename)
        {
            try
            {
                ConnectionFactory factory = new ConnectionFactory()
                {
                    HostName = HostName,
                    UserName = UserName,
                    Password = Password,
                    Port = Port
                };
                factory.RequestedHeartbeat = 10;
                factory.RequestedConnectionTimeout = 5000;               

                using (IConnection connection = factory.CreateConnection())
                {
                    using (IModel chanell = connection.CreateModel())
                    {

                        chanell.QueueDeclare(queue: queuename,
                                          durable: true,
                                          exclusive: false,
                                          autoDelete: false,
                                          arguments: null);

                        QueueingBasicConsumer consumer = new QueueingBasicConsumer(chanell);

                        chanell.BasicConsume(queuename, autoAck, consumer);
                        BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                        chanell.BasicAck(ea.DeliveryTag, false);
                        return "OK!";
                    }
                }
            }
            catch (Exception e)
            {
                return e.ToString();
            }
        }//Ack


        public string Nack(string queuename)
        {
            try
            {
                var factory = new ConnectionFactory()
                {
                    HostName = HostName,
                    UserName = UserName,
                    Password = Password,
                    Port = Port
                };

                factory.RequestedHeartbeat = 10;
                factory.RequestedConnectionTimeout = 5000;

                using (IConnection connection = factory.CreateConnection())
                {
                    using (IModel chanell = connection.CreateModel())
                    {
                        chanell.QueueDeclare(queue: queuename,
                                          durable: true,
                                          exclusive: false,
                                          autoDelete: false,
                                          arguments: null);

                        //var consumer = new EventingBasicConsumer(channel);
                        QueueingBasicConsumer consumer = new QueueingBasicConsumer(chanell);

                        chanell.BasicConsume(queuename, autoAck, consumer);
                        BasicDeliverEventArgs ea = (BasicDeliverEventArgs)consumer.Queue.Dequeue();

                        chanell.BasicNack(ea.DeliveryTag, false, false);
                        return "OK!";
                    }
                }
            }
            catch (Exception e)
            {
                return e.ToString();
            }
        }//Nack


        public string MessagesInQueueWC(string queuename)
        {
            string kol = "";
            try
            {
                IConnection connection = this.conn;
                IModel chanell = this.chan;
                using (connection)
                {
                    using (chanell)
                    {

                        QueueDeclareOk response = chanell.QueueDeclarePassive(queuename);
                        kol = response.MessageCount.ToString();
                        response = null;
                    }
                }
            }
            catch (Exception e)
            {
                kol = null;
                return e.ToString();
            }

            return kol;
        }

        public string MessagesInQueue(string queuename)
        {
            string kol = "";
            try
            {
                ConnectionFactory factory = new ConnectionFactory()
                {
                    UserName = UserName,
                    Password = Password,
                    HostName = HostName,
                    Port = Port
                };

                factory.RequestedHeartbeat = 10;
                factory.RequestedConnectionTimeout = 5000;

                using (IConnection connection = factory.CreateConnection())
                {
                    using (IModel chanell = connection.CreateModel())
                    {
                        QueueDeclareOk response = chanell.QueueDeclarePassive(queuename);
                        kol = response.MessageCount.ToString();
                        response = null;
                    }
                }
            }
            catch (Exception e)
            {
                kol = null;
                return e.ToString();
            }

            return kol;
        }

    }
}
