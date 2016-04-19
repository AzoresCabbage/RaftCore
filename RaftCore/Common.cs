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
    public class LogEntryStruct
    {
        #region Public Variable
        public string data { get { return data_; } }
        public int term { get { return term_; } }
        #endregion

        string data_;
        int term_;
        public LogEntryStruct(int _term, string _data)
        {
            term_ = _term;
            data_ = _data;
        }
    }
    public class ClusterNodeSocket
    {
        public int ID;
        public IPEndPoint IPPort;
        public TTransport transport;
        public ClusterNodeRPC.Client client;
        public ClusterNodeSocket(TTransport _transport, ClusterNodeRPC.Client _client, IPEndPoint _ipport, int id)
        {
            ID = id;
            transport = _transport;
            client = _client;
            IPPort = _ipport;
        }
    }

    public enum ElectionState
    {
        ElectionSuccess = 0,
        ElectionFailed = 1,
        HigherTerm = 2,
        Stop = 3,
    }

    public enum ServerState
    {
        FindLeader = 0,
        Timeout = 1,
        HigherTerm = 2,
        Running = 3,
    }

    public partial class ServerHandler : ClusterNodeRPC.Iface
    {
        #region Private Variable
        private Timer timeout_timer_;
        private int curRandTime_;
        private Random rand_;
        private List<ClusterNodeSocket> ExClusterNodeList_;
        private int leaderID_;
        private List<LogEntryStruct> log_;  //本node的日志
        private int[] nextIndex_;            //需要给其他node发送的下一条日志下标，初始值为log的最后一条下标+1
        private int[] matchIndex_;           //其他node已经安全复制的最后一个下标
        private int lastApplied_;            //最后应用的entry下标，如果lastApplied < commitIndex，则将log[lastApplied]置入状态机
        private int commitIndex_;            //当前已经提交的entry下标

        public int TestNum;

        #endregion

        #region Public Variable

        public bool isTimerAlive { get { return timeout_timer_ != null; } }

        public ElectionState election_state_;
        public volatile bool is_election_done_;

        public volatile bool leader_should_stop_;

        public ServerState state{get{return state_;}}
        private volatile ServerState state_;

        public int term{get{return term_;}}
        private volatile int term_;

        public NodeRole role { get { return role_; } }
        private volatile NodeRole role_;

        public bool isVoteInThisTerm { get { return isVoteInThisTerm_; } }
        private volatile bool isVoteInThisTerm_;

        public int id { get { return id_; } }
        private int id_;


        // log replication
        public int logLength { get { return log_.Count; } }
        #endregion

        

        public ServerHandler(int term, int _id, NodeRole _role, List<ClusterNodeSocket> _ExClusterNodeList)
        {

            TestNum = 0;

            id_ = _id;
            role_ = _role;
            term_ = term;
            state_ = ServerState.Running;
            isVoteInThisTerm_ = false;
            leader_should_stop_ = false;
            is_election_done_ = false;
            lastApplied_ = -1; //与论文不同，论文下标从1开始，所以初始值为0
            commitIndex_ = -1;
            election_state_ = ElectionState.Stop;
            ExClusterNodeList_ = new List<ClusterNodeSocket>();
            ExClusterNodeList_ = _ExClusterNodeList;
            nextIndex_ = new int[ExClusterNodeList_.Count];
            matchIndex_ = new int[ExClusterNodeList_.Count];
            log_ = new List<LogEntryStruct>();
            rand_ = new Random();
            int seed = rand_.Next() * _id;
            rand_ = new Random(seed);
        }

        public void StartTimer()
        {
            curRandTime_ = rand_.Next(Config.Heartbeat_interval*3, Config.Heartbeat_interval*4);
            Console.WriteLine(String.Format("{0} [{1}] current timer = {2}", id, role, curRandTime_));
            timeout_timer_ = new Timer(
                              (Object obj) => {
                                  lock (this)
                                  {
                                      role_ = NodeRole.Candidate;
                                      state_ = ServerState.Timeout;
                                      term_ = term_ + 1;
                                      isVoteInThisTerm_ = true;
                                      Console.WriteLine("{0} [{1}] ServerHandler {2}", id, role, state);
                                  }
                              },
                              null,
                              curRandTime_,
                              curRandTime_);
        }

        public void StopTimer()
        {
            //Console.WriteLine(String.Format("{0} [{1}] timer stopped", id, role));
            timeout_timer_.Dispose();
            timeout_timer_ = null;
        }

        public void ToBeLeader()
        {
            role_ = NodeRole.Leader;
        }

        public void ToBeFollower()
        {
            role_ = NodeRole.Follower;
        }

        public void ToBeRunning()
        {
            state_ = ServerState.Running;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="LeaderID">发送此heartbeat的leaderID</param>
        /// <param name="term">leader所处的term</param>
        /// <param name="prevLogIndex">当前entry之前一个log的index</param>
        /// <param name="prevLogTerm">当前entry之前一个log的term</param>
        /// <param name="entry">当前需要储存的entry</param>
        /// <param name="leaderCommit">leader的commitIndex</param>
        /// <returns></returns>
        public bool heartbeat(int LeaderID, int term, int prevLogIndex, int prevLogTerm, string entry, int leaderCommit)
        {
            lock (this)
            {
                if (term < term_) // any role recv small term should dispose
                {
                    Console.WriteLine(String.Format("{0} [{1}] Receive small term from {2} (recv_term vs term) {3} < {4}", id, role, LeaderID, term, term));
                    return false;
                }
                leaderID_ = LeaderID;
                if (term > term_)// any role recv HigherTerm should become follower
                {
                    Console.WriteLine(String.Format("{0} [{1}] Receive big term from {2} (recv_term vs term) {3} > {4}", id, role, LeaderID, term, term));
                    if (role_ == NodeRole.Follower)// if was follower, just reset timer
                    {
                        if (timeout_timer_ != null)
                            timeout_timer_.Change(curRandTime_, curRandTime_);
                        state_ = ServerState.Running;
                    }
                    role_ = NodeRole.Follower;
                    state_ = ServerState.HigherTerm;
                    term_ = term;
                    isVoteInThisTerm_ = false;
                }
                else // same term heartbeat
                {
                    if (role == NodeRole.Follower)
                    {
                        Console.WriteLine(String.Format("{0} [{1}] Receive HeartBeat from {2}, term = {3}", id, role, LeaderID, term));
                        if (timeout_timer_ != null)
                            timeout_timer_.Change(curRandTime_, curRandTime_);
                        // role not change
                        state_ = ServerState.Running;
                        // term not change
                        // vote not change
                    }
                    else if (role == NodeRole.Candidate)
                    {
                        // 重新发现了现在的leader
                        Console.WriteLine(String.Format("{0} [{1}] Receive HeartBeat from {2}, term = {3}", id, role, LeaderID, term));

                        role_ = NodeRole.Follower;
                        state_ = ServerState.FindLeader;
                        // term not change
                        // vote not change
                    }
                    else
                    {
                        Console.WriteLine(String.Format("{0} [{1}] I'am Leader, are you kidding me? {2} (recv_term vs term) {3} == {4}", id, role, LeaderID, term, term));
                    }
                    //Console.WriteLine(String.Format("{0} [{1}] Receive HeartBeat from {2}, term = {3}", id, role, LeaderID, term));
                }
                
                if (leaderCommit > commitIndex_)
                    commitIndex_ = Math.Min(leaderCommit, log_.Count - 1);

                if(entry.Equals(""))
                    return true; //for heartbeat, can return any value
                else
                {
                    if (prevLogIndex >= log_.Count) // bigger than current last log index
                        return false;
                    else if(prevLogIndex == log_.Count - 1) // new entry just simply add it to log
                    {
                        log_.Add(new LogEntryStruct(term, entry));
                        return true;
                    }
                    else
                    {
                        if (prevLogIndex < 0)
                            return false;
                        if (log_[prevLogIndex].term == prevLogTerm) //$5.3 rule 1: same index and term -> same log
                        {
                            return true;
                        }
                        else
                        {
                            log_.RemoveRange(prevLogIndex, log_.Count - prevLogIndex);
                            return false;
                        }
                    }
                }
            }
        }

        public VoteBackStruct voteForMe(int term, int CandidateId, int lastLogIdx, int lastLogTerm)
        {
            lock (this)
            {
                VoteBackStruct ret = new VoteBackStruct();
                ret.Term = term_;
                ret.VoteGranted = false;
                if (term > term_) // recv higher term request
                {
                    Console.WriteLine(String.Format("{0} [{1}] voted in term {2}, recv_term {3} from {4}, isVoteInThisTerm_ {5}!", id, role, term, term, CandidateId, isVoteInThisTerm_));
                    role_ = NodeRole.Follower;
                    state_ = ServerState.HigherTerm;
                    term_ = term;
                    isVoteInThisTerm_ = false;
                    if (lastLogTerm < getLastLogTerm()) //log最后的term比我小
                        return ret;
                    if (lastLogTerm == getLastLogTerm() && lastLogIdx < log_.Count) //log最后的term和我一样，但是log没有我多
                        return ret;
                    //if(timeout_timer_ != null)
                    //    timeout_timer_.Change(curRandTime_, curRandTime_);
                    ret.VoteGranted = isVoteInThisTerm_ = true;
                    return ret;
                }
                else
                {
                    ret.VoteGranted = false;
                    return ret;
                }
            }
        }

        public bool appendLogEntry(string data) //apply以后才能返回
        {
            int curLastLogIndex = log_.Count - 1;
            
            LOG(String.Format("receive command : {0}", data));
            //不能lock this，因为此node调用leader，leader在同步时，由于此node被lock，导致无法接收数据出错
            if (role != NodeRole.Leader)
            {
                for (int i = 0; i < ExClusterNodeList_.Count; ++i)
                {
                    if (ExClusterNodeList_[i].transport.IsOpen && leaderID_ == ExClusterNodeList_[i].ID)
                    {
                        bool res = ExClusterNodeList_[i].client.appendLogEntry(data);
                        LOG("appendLogEntry callback done!");
                        return res;
                    }
                }
            }
            lock (this)
            {
                log_.Add(new LogEntryStruct(term, data));
            }
            while (curLastLogIndex + 1 > lastApplied_)
            {
                continue;
            }
            return true;

        }

        public int getTestNum()
        {
            lock (this)
            {
                LOG("getTestNum");
                if (role != NodeRole.Leader)
                {
                    for (int i = 0; i < ExClusterNodeList_.Count; ++i)
                    {
                        if (ExClusterNodeList_[i].transport.IsOpen && leaderID_ == ExClusterNodeList_[i].ID)
                        {
                            return ExClusterNodeList_[i].client.getTestNum();
                        }
                    }
                }
            }
            return TestNum;
        }

        public void ParseLog()
        {
            ++lastApplied_;
            string curCommand = log_[lastApplied_].data;
            string[] tmp = curCommand.Split(' ');
            if (tmp[0] == "+")
            {
                TestNum += int.Parse(tmp[1]);
            }
            else if (tmp[0] == "-")
            {
                TestNum -= int.Parse(tmp[1]);
            }
            else //times
            {
                TestNum *= int.Parse(tmp[1]);
            }
        }

        public void commit()
        {
            if (lastApplied_ < commitIndex_)
            {
                lock (this)
                {
                    //TODO() :
                    // apply log to state machine
                    ParseLog();
                }
            }
        }
    }

    public class Config
    {
        static Random rand = new Random();
        public const int Max_Reconnect_Num = 5;
        public const int Heartbeat_interval = 500;//ms
    }


}