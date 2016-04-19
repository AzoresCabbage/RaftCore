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
        public void candidate_election()
        {
            election_state_ = candidate_vote_sender();
            is_election_done_ = true;
        }

        public ElectionState candidate_vote_sender()
        {
            LOG(String.Format("term {0} calling for votes", term));

            int alive_num = 1;
            int votes = 1;
            VoteBackStruct ret = new VoteBackStruct();
            foreach (ClusterNodeSocket node in ExClusterNodeList_)
            {
                try
                {
                    if (node.transport.IsOpen)
                    {
                        ++alive_num;
                        ret = node.client.voteForMe(term, id, log_.Count, getLastLogTerm());
                        if (ret.VoteGranted)
                            ++votes;
                        if (ret.Term > term)
                        {
                            return ElectionState.HigherTerm;
                        }
                        //Console.WriteLine(String.Format("{0} [{1}] term {2} calling for votes", ID, curRole_, curTerm_));
                    }
                }
                catch (Exception ex)
                {
                    LOG(String.Format("get votes error! {0} {1}", leader_should_stop_, ex.Message));
                    if (leader_should_stop_)
                        return ElectionState.Stop;
                }
            }
            LOG(String.Format("get votes {0}", votes));
            if (votes > alive_num / 2)
            {
                return ElectionState.ElectionSuccess;
            }
            else
            {
                return ElectionState.ElectionFailed;
            }
        }
        
    }
}