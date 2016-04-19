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

    public partial class ServerHandler
    {
        public class HeartbeatSenderWrapper
        {
            ClusterNodeSocket node;
            string data;
            public HeartbeatSenderWrapper(ClusterNodeSocket _node, string _data)
            {
                node = _node;
                data = _data;
            }
        }
        public delegate bool heartbeatSenderDelegate(HeartbeatSenderWrapper obj);
        public bool heartbeatSender(HeartbeatSenderWrapper obj)
        {
            return true;
        }

        // we can't statistic acks got in one round to judge whether commit, because leader 
        // may send different log to others
        public void leader_heartbeat_sender()
        {
            while (!leader_should_stop_)
            {
                int alive = 0;
                int bigger_than_commitIndex_num = 0;
                for (int i = 0; i < ExClusterNodeList_.Count; ++i)
                {
                    ClusterNodeSocket node = ExClusterNodeList_[i];
                    //heartbeatSenderDelegate del = new heartbeatSenderDelegate(heartbeatSender);
                    //IAsyncResult res = del.BeginInvoke(new HeartbeatSenderWrapper(node, ""),null,Object());

                    if (node.transport.IsOpen)
                    {
                        ++alive;
                        try
                        {
                            string send_content = "";
                            if (nextIndex_[i] < log_.Count)
                                send_content = log_[nextIndex_[i]].data;
                            // send heartbeat or log entry
                            // if nextIndex_[i] - 1 == -1, the server will process it correctly no matter what value the getLogTermAt is
                            bool res = node.client.heartbeat(id, term, nextIndex_[i] - 1, getLogTermAt(nextIndex_[i] - 1), send_content, commitIndex_);
                            if (send_content != "")
                            {
                                if (res)
                                {
                                    ++matchIndex_[i];
                                    ++nextIndex_[i];
                                    if(matchIndex_[i] > commitIndex_)
                                        ++bigger_than_commitIndex_num;
                                }
                                else
                                {
                                    --nextIndex_[i];
                                }
                            }
                            else
                            {
                                //do nothing for heaartbeat
                            }
                        }
                        catch (Exception ex)
                        {
                            LOG(String.Format("heartbeat error {0} {1}", ex.Message, ex.Data));
                        }
                    }
                }
                LOG(String.Format("One round({0}) heartbeat sent done", term));
                // if commitIndex + 1 > log's last index, than first condition would not meet
                if ( (ExClusterNodeList_.Count == 0 && commitIndex_  < log_.Count - 1)
                    ||
                    (bigger_than_commitIndex_num > alive / 2
                    && commitIndex_ + 1 < log_.Count
                    && log_[commitIndex_ + 1].term == term)
                    )
                {
                    ++ commitIndex_;
                }
                Thread.Sleep(Config.Heartbeat_interval);
            }
            Console.WriteLine(String.Format("{0} Leard heartbeat sender thread exit", id));
        }

    }
}