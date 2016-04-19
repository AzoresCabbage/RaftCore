using RaftRPC;
using Thrift;
using Thrift.Protocol;
using Thrift.Server;
using Thrift.Transport;
using System.Net;
using System.Threading;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaftCore
{
    class Program
    {
        public static bool exit_one = false;

        const int NodeNum = 2;
        public static Random rand = new Random();
        public static void start_thread(Object node)
        {
            ((ClusterNode)node).start();
            int randTime = rand.Next(5000, 10000);
            Console.WriteLine(randTime);
            Thread.Sleep(randTime);
            if (!exit_one)
            {
                exit_one = true;
                ((ClusterNode)node).stop();
            }
        }

        public class Test
        {
            public void print()
            {
                lock (this)
                {
                    for (int i = 0; i < 10000; ++i)
                        Console.Write(i);
                }
            }
            Thread th1, th2;
            public Test()
            {
                th1 = new Thread(print);
                th2 = new Thread((Object obj) =>
                {
                    lock (this)
                    {
                        Thread.Sleep(10000);
                    }
                });
            }
            public void start()
            {
                th2.Start();
                th1.Start();
            }
        }

        public static void multinode()
        {
            List<ClusterNode> node = new List<ClusterNode>();
            List<IPEndPoint> nodelist = new List<IPEndPoint>();
            List<Thread> th = new List<Thread>();
            List<int> IdList = new List<int>();
            for (int i = 0; i < NodeNum; ++i)
            {
                IdList.Add(i + 1);
                nodelist.Add(new IPEndPoint(IPAddress.Loopback, 9090 + i));
            }
            for (int i = 0; i < NodeNum; ++i)
            {
                node.Add(new ClusterNode(nodelist[i], nodelist, IdList));
            }

            for (int i = 0; i < NodeNum; ++i)
            {
                th.Add(new Thread(start_thread));
            }
            for (int i = 0; i < NodeNum; ++i)
            {
                th[i].Start(node[i]);
            }
            for (int i = 0; i < NodeNum; ++i)
            {
                th[i].Join();
            }
        }

        public static void oneNode(int id, IPEndPoint myip, List<IPEndPoint> nodelist, List<int> IdList)
        {
            ClusterNode node = new ClusterNode(myip, nodelist, IdList);
            node.start();
        }

        public static void Main()
        {
            List<IPEndPoint> nodelist = new List<IPEndPoint>();
            List<int> IdList = new List<int>();
            for (int i = 0; i < NodeNum; ++i)
            {
                nodelist.Add(new IPEndPoint(IPAddress.Loopback, 9090 + i));
                IdList.Add(i+1);
            }
            int port = int.Parse(Console.ReadLine());
            IPEndPoint myIP = new IPEndPoint(IPAddress.Loopback, port);
            oneNode(port - 9089, myIP, nodelist, IdList);
        }
    }
}
