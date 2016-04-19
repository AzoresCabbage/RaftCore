

namespace csharp RaftRPC

enum NodeRole {
  Follower = 1,
  Candidate = 2,
  Leader = 3
}

struct VoteBackStruct
{
	1: i32 term,
	2: bool voteGranted,
}

# what leader should do
# leader = client
# follower = server
service ClusterNodeRPC {
	bool heartbeat(1:i32 LeaderID, 2:i32 term, 3:i32 prevLogIndex, 4:i32 prevLogTerm, 5:string entry, 6:i32 leaderCommit),
	VoteBackStruct voteForMe(1:i32 term, 2:i32 CandidateId, 3:i32 lastLogIdx, 4:i32 lastLogTerm),
	bool appendLogEntry(1:string data),
	i32 getTestNum();
#	bool DisposeConnectionWithMe(),
#	bool syncBool(1:bool data),
#	bool syncByte(1:byte data),
#	bool syncI16(1:i16 data),
#	bool syncI32(1:i32 data),
#	bool syncI64(1:i64 data),
#	bool syncDouble(1:double data),
#	bool syncString(1:string data),
#	bool syncBinary(1:binary data)
}