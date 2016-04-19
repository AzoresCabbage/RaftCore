using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RaftRPC;
using Thrift;
using Thrift.Protocol;
using Thrift.Server;
using Thrift.Transport;

namespace ClientTest
{

    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                TTransport transport = new TSocket("localhost", 9091);
                TProtocol protocol = new TBinaryProtocol(transport);
                ClusterNodeRPC.Client client = new ClusterNodeRPC.Client(protocol);

                transport.Open();
                try
                {
                    client.appendLogEntry("+ 5");
                    Console.WriteLine("+ 5");
                    client.appendLogEntry("* 3");
                    Console.WriteLine("* 3");
                    client.appendLogEntry("- 2");
                    Console.WriteLine("- 2");
                    int res = client.getTestNum();
                    Console.WriteLine(String.Format("res is {0}", res));
                }
                finally
                {
                    transport.Close();
                }
            }
            catch (TApplicationException x)
            {
                Console.WriteLine(x.StackTrace);
            }
        }
    }
}
