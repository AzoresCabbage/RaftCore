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

        private void connect()
        {
            Console.WriteLine("Connecting thread running...");
            while (!node_should_stop_)
            {
                for (int i = 0; i < ExClusterNodeList_.Count; ++i)
                {
                    ClusterNodeSocket tmp = ExClusterNodeList_[i];
                    if (tmp.transport.IsOpen)
                        continue;
                    Console.WriteLine(String.Format("Connecting to {0}:{1}...",
                            OriginalIPEndPointList_[i].Address.ToString(),
                            OriginalIPEndPointList_[i].Port));
                    try
                    {
                        tmp.transport.Open();
                        Console.WriteLine(String.Format("success: Connecting to {0}:{1}",
                            OriginalIPEndPointList_[i].Address.ToString(),
                            OriginalIPEndPointList_[i].Port));
                    }
                    catch(TimeoutException ex)
                    {
                        Console.WriteLine(String.Format("Failed: Connecting to {0}:{1} {2}",
                            OriginalIPEndPointList_[i].Address.ToString(),
                            OriginalIPEndPointList_[i].Port, ex.Message));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(String.Format("Failed: Connecting to {0}:{1} {2}",
                            OriginalIPEndPointList_[i].Address.ToString(),
                            OriginalIPEndPointList_[i].Port, ex.Message));
                        tmp.transport = new TSocket(tmp.IPPort.Address.ToString(), tmp.IPPort.Port, Config.Heartbeat_interval * 2); //timeout ms
                        TProtocol protocol = new TBinaryProtocol(tmp.transport);
                        tmp.client = new ClusterNodeRPC.Client(protocol);
                    }
                }
                //Thread.Sleep(500);
            }
            Console.WriteLine("{0} Connecting thread exited...", ServerHandler_.id);
        }

        private void server_thread()
        {
            try
            {
                Server_.Serve();
            }
            catch (Exception ex)
            {
                Console.WriteLine(String.Format("server error : {0}",ex.Message));
            }
        }

        private void state_checker()
        {
            Console.WriteLine("state checker thread running...");
            while (!node_should_stop_)
            {
                if (ServerHandler_.is_election_done_)
                {
                    lock(this)
                    {
                        judge_Election();
                    }
                }
                lock (this)
                {
                    switch (ServerHandler_.state) //judge internal state(state can be verify in heartbeat and vote)
                    {
                        case ServerState.Running:
                            // everything is ok!
                            break;
                        case ServerState.FindLeader:
                            judge_FindLeader();
                            break;
                        case ServerState.Timeout:
                            judge_Timeout();
                            break;
                        case ServerState.HigherTerm:
                            judge_HigherTerm();
                            break;
                        default:
                            ServerHandler_.LOG(String.Format("Unknown state {0}", ServerHandler_.state));
                            break;
                    }
                }
            }
            Console.WriteLine("{0} state checker thread existed!", ServerHandler_.id);
        }

        private void commit()
        {
            while(!node_should_stop_)
            {
                ServerHandler_.commit();
            }
        }

        private void judge_FindLeader()
        {
            if (ServerHandler_.role == NodeRole.Follower)
            {
                lock (this)
                {
                    StopThread();
                    SwitchThread();
                    StartThread();
                    ServerHandler_.ToBeRunning();
                }
            }
            else
            {
                ServerHandler_.LOG(String.Format("get FindLeader state"));
            }
        }

        private void judge_Election()
        {
            StopThread();
            switch (ServerHandler_.election_state_)
            {
                case ElectionState.ElectionFailed:
                    ServerHandler_.ToBeFollower();
                    break;
                case ElectionState.ElectionSuccess:
                    ServerHandler_.ToBeLeader();
                    ServerHandler_.reInitLeader();
                    break;
                case ElectionState.HigherTerm:
                    ServerHandler_.ToBeFollower();
                    break;
                case ElectionState.Stop:
                    ServerHandler_.ToBeFollower();
                    break;
            }
            SwitchThread();
            StartThread();
            ServerHandler_.is_election_done_ = false;
        }

        private void judge_HigherTerm()
        {
            // 引发higher term的时候就已经把role改为了follower
            if(ServerHandler_.role != NodeRole.Follower)
            {
                // do nothing
            }
            else
            {
                lock (this)
                {
                    StopThread();
                    ServerHandler_.ToBeFollower();
                    SwitchThread();
                    StartThread();
                    ServerHandler_.ToBeRunning();
                }
            }
        }

        private void judge_Timeout()
        {
            if (ServerHandler_.role == NodeRole.Candidate)
            {
                lock (this)
                {
                    StopThread();
                    SwitchThread();
                    StartThread();
                    ServerHandler_.ToBeRunning();
                }
            }
            else
            {
                ServerHandler_.LOG(String.Format("get Timeout state"));
            }
        }

        
    }


    public partial class ServerHandler
    {
        public void LOG(String str)
        {
            Console.WriteLine(String.Format("{0} [{1}] {2}", id, role, str));
        }
        public int getLastLogTerm()
        {
            if (log_.Count > 0)
                return log_.Last<LogEntryStruct>().term;
            else
                return 0;
        }

        public int getLogTermAt(int idx)
        {
            if (log_.Count <= idx || idx < 0)
                return 0;
            else
                return log_[idx].term;
        }

        public void reInitLeader()
        {
            for (int idx = 0; idx < nextIndex_.Length; ++idx)
            {
                nextIndex_[idx] = log_.Count; //last log index，与论文不同，论文下标从1开始
                matchIndex_[idx] = -1;
            }
        }
    }
}