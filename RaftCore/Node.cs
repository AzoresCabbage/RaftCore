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
    public partial class ClusterNode
    {
        #region Variable
        volatile bool node_should_stop_;

        IPEndPoint myIPandPort_;
        List<IPEndPoint> OriginalIPEndPointList_;
        List<ClusterNodeSocket> ExClusterNodeList_;

        Thread TStateChecker_;
        Thread TServer_;
        Thread TClient_;
        Thread Tconnect_;
        Thread Tcommit_;

        ServerHandler ServerHandler_;
        TServerTransport ServerTransport_;
        TServer Server_;

        #endregion

        public ClusterNode(IPEndPoint myIPandPort, List<IPEndPoint> ClusterNodeList, List<int> nodeIdList)
        {
            //设定各个状态的初始值
            myIPandPort_ = myIPandPort;
            node_should_stop_ = false;
            

            // 获取其他所有节点的transport和client，存于ExClusterNodeList，不包括自身
            ExClusterNodeList_ = new List<ClusterNodeSocket>();
            OriginalIPEndPointList_ = new List<IPEndPoint>();
            int _id = -1;
            for(int i=0;i<ClusterNodeList.Count;++i)
            {
                IPEndPoint IPPort = ClusterNodeList[i];
                if (myIPandPort_.Equals(IPPort))
                {
                    _id = nodeIdList[i];
                    continue; 
                }
                
                OriginalIPEndPointList_.Add(IPPort);
                TTransport transport = new TSocket(IPPort.Address.ToString(), IPPort.Port, Config.Heartbeat_interval*2); //timeout ms
                TProtocol protocol = new TBinaryProtocol(transport);
                ClusterNodeRPC.Client client = new ClusterNodeRPC.Client(protocol);
                ExClusterNodeList_.Add(new ClusterNodeSocket(transport, client, IPPort, nodeIdList[i]));
            }

            //初始化Server
            ServerHandler_ = new ServerHandler(0, _id, NodeRole.Follower, ExClusterNodeList_);
            ClusterNodeRPC.Processor processor = new ClusterNodeRPC.Processor(ServerHandler_);
            ServerTransport_ = new TServerSocket(myIPandPort.Port);
            Server_ = new TThreadPoolServer(processor, ServerTransport_);

            // 初始化线程
            TServer_ = new Thread(server_thread);
            Tconnect_ = new Thread(connect);
            TStateChecker_ = new Thread(state_checker);
            Tcommit_ = new Thread(commit);
        }

        private void SwitchThread()
        {
            switch(ServerHandler_.role)
            {
                case NodeRole.Follower:
                    break;
                case NodeRole.Leader:
                    TClient_ = new Thread(ServerHandler_.leader_heartbeat_sender);
                    break;
                case NodeRole.Candidate:
                    TClient_ = new Thread(ServerHandler_.candidate_election);
                    break;
            }
        }

        private void StartThread()
        {
            if(ServerHandler_.role == NodeRole.Leader)
                ServerHandler_.leader_should_stop_ = false;
            if (null != TClient_ && NodeRole.Follower != ServerHandler_.role)
            {
                try
                {
                    TClient_.Start();
                }
                catch(Exception ex)
                {
                    ServerHandler_.LOG(String.Format("client start error {0}", ex.Message));
                }
            }
            if (null != TServer_)
            {
                if (NodeRole.Follower == ServerHandler_.role)
                {
                    ServerHandler_.LOG("StartTimer");
                    ServerHandler_.StartTimer();
                }
            }
        }

        private void StopThread()
        {
            ServerHandler_.leader_should_stop_ = true;
            if (null != TClient_ && NodeRole.Follower != ServerHandler_.role)
            {
                try
                {
                    TClient_.Interrupt();
                    TClient_.Abort();
                    TClient_.Join();
                }
                catch (Exception ex)
                {
                    ServerHandler_.LOG(String.Format("client thread aborted! Msg:{0}", ex.Message));
                }
            }
            if (TServer_.IsAlive && ServerHandler_.isTimerAlive)
            {
                ServerHandler_.LOG("StopTimer");
                ServerHandler_.StopTimer();
            }
        }

        public void start()
        {
            node_should_stop_ = false;
            TServer_.Start();
            Tconnect_.Start();
            TStateChecker_.Start();
            Tcommit_.Start();
            
            SwitchThread();
            StartThread();
        }

        public void stop()
        {
            ////try
            //{
            //    lock (this)
            //    {

            //        LOG("existing thread...");
            //        node_should_stop_ = true;
            //        Tconnect_.Abort();
            //        Tconnect_.Join();
            //        Thread.Sleep(1000);

            //        foreach (ClusterNodeSocket tmp in ExClusterNodeList_)
            //        {
            //            if (tmp.transport.IsOpen)
            //            {
            //                tmp.client.DisposeConnectionWithMe();
            //                tmp.transport.Close();
            //                tmp.client.Dispose();
            //                LOG("close one client");
            //            }
            //        }
            //        ServerTransport_.Close();
            //        Server_.Stop();
            //        //Console.WriteLine("{0} stop thread done!", ServerHandler_.id);
            //        //GC.Collect();
            //    }
            //}
            ////catch
            ////{
            ////    Console.WriteLine("Exception occur when thread exit");
            ////}
        }

    }
}