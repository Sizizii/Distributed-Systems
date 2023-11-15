
import asyncio
import sys
import collections
import threading
import random
from enum import Enum
from collections import defaultdict as df

import logging
import alog

# state type for raft nodes
class state(Enum):
    LEADER = 1
    FOLLOWER = 2
    CANDIDATE = 3

# timeout ranges are usually set as
# heartBeat: 100ms, timeout: 150ms-300ms
# we set in our raft:
#   heartBeat: 1
#   timeout: 2-4
heartBeat = 1
neverTimeout = 500

class Raft():
    
    def __init__(self, nodeId, total) -> None:
        self.id = nodeId
        self.timeOut = random.random()*2 + heartBeat*2

        # for term & state
        self.curTerm = 1
        self.nodeState = state.FOLLOWER
        self.votedFor = None
        self.voteCount = 0

        # if State is updated, print variables that need to be tracked: term, state, leader, log, commitIndex
        self.StateUpdate = 1    


        #for LOGs & commit, suitable for LEADER state
        self.commitIndex = 0 
        self.nodeCommit = {}    # check commit consistency: if maximum log id matches
        for nid in range(total):
            if nid != self.id:
                self.nodeCommit[nid] = 0

        self.log = [""]
        self.nodeLog = df(set)  # check if ready for commit


    def sendRequestVote(self):
        #timeout, start election
        self.curTerm += 1
        self.nodeState = state.CANDIDATE
        self.voteCount = 1
        self.votedFor = self.id

        # track state: CANDIDATE, term
        self.StateUpdate = 1
        print(f'STATE state="{self.nodeState}"', flush=True)
        print(f"STATE term={self.curTerm}", flush=True)
        # print(f"STATE leader=null", flush=True) #added
        for nid in self.nodeCommit.keys():
            print(f"SEND {nid} RequestVotes {self.curTerm} {self.commitIndex}", flush=True)

    def recvRequestVote(self, candidateId, candidateTerm, candidateCommit):
        # logging.info("Term change, candidateId: %d, candidateTerm: %d, id: %d, curTerm: %d" % (candidateId, candidateTerm, self.id, self.curTerm))

        if candidateTerm < self.curTerm:
            pass
        
        elif candidateTerm > self.curTerm:
            if candidateTerm > self.curTerm + 1:
                # Exception
                return
            # vote for candidate
            self.curTerm = candidateTerm
            self.nodeState = state.FOLLOWER
            self.voteCount = 0
            self.votedFor = candidateId
            self.StateUpdate = 1
            # reply vote to candidate
            print(f"SEND {candidateId} RequestVotesResponse {self.curTerm} true {self.commitIndex}", flush=True)
            self.timeOut = random.random()*2 + heartBeat*2


        elif candidateTerm == self.curTerm and self.nodeState != state.LEADER:
            if (self.votedFor is not None and self.votedFor is not candidateId) or self.nodeState == state.CANDIDATE or candidateCommit < self.commitIndex:
                print(f"SEND {candidateId} RequestVotesResponse {self.curTerm} false {self.commitIndex}", flush=True)
            else:
                self.nodeState = state.FOLLOWER
                self.voteCount = 0
                self.votedFor = candidateId
                print(f"SEND {candidateId} RequestVotesResponse {self.curTerm} true {self.commitIndex}", flush=True)
                self.timeOut = random.random()*2 + heartBeat*2

    def recvVote(self, nodeId, nodeTerm, nodeReply, nodeCommit):
        if nodeTerm > self.curTerm:
            self.curTerm = nodeTerm
            self.nodeState = state.FOLLOWER
            self.voteCount = 0
            self.votedFor = nodeId
        elif nodeTerm == self.curTerm and self.nodeState == state.CANDIDATE:
            self.nodeCommit[nodeId] = nodeCommit
            if nodeReply:
                self.voteCount += 1
                # print("Increment Vote", self.voteCount)
                if self.voteCount >= total //2 + 1:
                    for logId in range(self.commitIndex, len(self.log)):
                        self.nodeLog[logId].add(self.id)

                    self.nodeState = state.LEADER
                    self.timeOut = neverTimeout
                    print(f'STATE state="{self.nodeState.name}"', flush=True)
                    print(f"STATE leader={self.id}", flush=True)

                    return
        self.timeOut = random.random()*2 + heartBeat*2

    def recvAppendEntries(self, leaderId, leaderTerm, appendType, logIndex, logMsg):
        #if logIndex != 0 and logIndex != None:
        #   raise IndexError(logIndex)
        # logging.info("id: %d, term: %d, commitidx: %d" % (self.id, self.curTerm, self.commitIndex))
    
        if leaderTerm < self.curTerm:
            # logging.info("Term change, leaderId: %d, leaderTerm: %d, id: %d, curTerm: %d" % (leaderId, leaderTerm, self.id, self.curTerm))
            self.curTerm = leaderTerm
            return
        elif leaderTerm == self.curTerm:
            if leaderId == self.id:
                return
            self.curTerm = leaderTerm
            self.nodeState = state.FOLLOWER
            self.voteCount = 0
            self.votedFor = leaderId
            if self.StateUpdate:
                # print(f"STATE leader=null", flush=True) #added
                print(f"STATE term={self.curTerm}", flush=True)
                print(f'STATE state="{self.nodeState}"', flush=True)
                print(f"STATE leader={leaderId}", flush=True)
                # print(f"SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex}", flush=True)
                self.StateUpdate = 0
            
            if appendType == 0:
                # hearbeat
                print(f"SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex}", flush=True)
            elif appendType == 1:
                # log append
                if logIndex > self.commitIndex and logMsg not in self.log:
                    # logging.info("p1")

                    if len(self.log) >= self.commitIndex+1:
                        self.log.pop()
                    elif logIndex ==  self.commitIndex+1:
                        self.log.append(logMsg)
                        
                        # logging.info("Check: Leader: %d, leaderTerm: %d, id: %d, commitIndex: %d, term: %d, log_len: %d" % (leaderId, leaderTerm, self.id, self.commitIndex, self.curTerm, len(self.log)))
                        # logging.info(self.log)
                        print(f'STATE log[{self.commitIndex+1}]=[{self.curTerm},"{logMsg}"]', flush=True)

                        print(f'STATE commitIndex={self.commitIndex}', flush=True)

                    print(f"SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex}", flush=True)
                elif logMsg not in self.log:
                    # logging.info("p2")

                    # while self.commitIndex > logIndex:
                    #     # delete extra entries
                    #     self.log.pop(-1)
                    #     self.commitIndex -= 1
                    self.log.append(logMsg)
                    print(f'STATE log[{self.commitIndex+1}]=[{self.curTerm},"{logMsg}"]', flush=True)
                    # logging.info(f'{self.id}: STATE log[{self.commitIndex+1}]=[{self.curTerm},"{logMsg}"]')
                    print(f"SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex} true", flush=True)
                    # logging.info(f"{self.id}: SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex} true")
                    # self.commitIndex += 1
                elif logIndex > self.commitIndex:
                    # logging.info("p3")
                    self.commitIndex += 1
                    print(f'STATE commitIndex={self.commitIndex}', flush=True)
                    # logging.info(f'{self.id}: STATE commitIndex={self.commitIndex}, {self.commitIndex}')
                    print(f"SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex} false", flush=True)
                    # logging.info(f'{self.id}: SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex} false')
                else:
                    print(f"SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex}", flush=True)
        elif leaderTerm > self.curTerm:
            # logging.info("Term change, leaderId: %d, leaderTerm: %d, id: %d, curTerm: %d" % (leaderId, leaderTerm, self.id, self.curTerm))

            self.curTerm = leaderTerm
            self.nodeState = state.FOLLOWER
            self.voteCount = 0
            self.votedFor = leaderId
            # print(f"STATE leader=null", flush=True) #added
            print(f"STATE term={self.curTerm}", flush=True)
            print(f'STATE state="{self.nodeState}"', flush=True)
            print(f"STATE leader={leaderId}", flush=True)
            print(f"SEND {leaderId} AppendEntriesResponse {self.curTerm} {self.commitIndex}", flush=True)
        self.timeOut = random.random()*2 + heartBeat*2

    def commitEntries(self, nodeId, nodeCommit, ifAppend):
        
        if ifAppend != None:
            # logging.info("Here1")
            if ifAppend == "true":
                if nodeCommit == self.commitIndex and (self.commitIndex < (len(self.log) - 1)):
                    self.nodeLog[self.commitIndex].add(nodeId)
                    # self.nodeCommit[nodeId] = self.commitIndex
            else:
                self.nodeCommit[nodeId] += 1

                
        else:
            # logging.info("Here2")
            self.nodeCommit[nodeId] = nodeCommit


async def heartbeat():
    while 1:
        if raft.nodeState == state.LEADER:
            # check commit messages
            if raft.commitIndex + 1 < len(raft.log) and (len(raft.nodeLog[raft.commitIndex]) >= total //2 + 1):
                raft.commitIndex += 1
                print(f"STATE commitIndex={raft.commitIndex}", flush=True)
                # logging.info(f"{raft.id}: STATE commitIndex={raft.commitIndex}")
                print(f"COMMITTED {raft.log[raft.commitIndex]} {raft.commitIndex}", flush=True)
                # logging.info(f"{raft.id}: COMMITTED {raft.log[raft.commitIndex]} {raft.commitIndex}")

            # send AppendEntries or heartbeat to peers
            for nid in raft.nodeCommit.keys():
                # all append, send heartbeat
                if (raft.commitIndex== len(raft.log)- 1 and raft.commitIndex== raft.nodeCommit[nid]):
                    print(f"SEND {nid} AppendEntries {raft.curTerm}", flush=True)
                # follower needs to catch up with leader
                elif raft.nodeCommit[nid] < raft.commitIndex:
                    #logging.info(nid)
                    next_idx =raft.nodeCommit[nid] + 1
                    next_log = raft.log[next_idx]
                    # logging.info(nid)
                    # logging.info(raft.nodeCommit[nid])
                    print(f'SEND {nid} AppendEntries {raft.curTerm} {next_idx} ["{next_log}"]', flush=True)
                    # logging.info(f'{raft.id}: SEND {nid} AppendEntries {raft.curTerm} {next_idx} ["{next_log}"]')
                # leader has sth. left to be committed
                elif raft.nodeCommit[nid] == raft.commitIndex:
                    #raise IndexError(raft.commitIndex)
                    # print(raft.commitIndex, len(raft.log))
                    next_log = raft.log[raft.commitIndex + 1]
                    print(f'SEND {nid} AppendEntries {raft.curTerm} {raft.commitIndex} ["{next_log}"]', flush=True)
        await asyncio.sleep(heartBeat)





    



async def raft_process():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    async def read_msg():
        nonlocal reader
        stdin = await reader.readline()
        msg = stdin.decode('utf-8')
        return msg
            
    while True:
        try:

            msg = await asyncio.wait_for(read_msg(), raft.timeOut)
            msg = msg.strip()
            command = msg.split(" ")

            if command[0] == "LOG":
                if raft.nodeState == state.LEADER:
                    content = command[1]
                    print(f"LOG {content}", flush = True)
                    # logging.info(f"{raft.id}: LOG {content}")
                    print(f'STATE log[{len(raft.log)}]=[{raft.curTerm},"{content}"]', flush = True)
                    # logging.info(f'{raft.id}: STATE log[{len(raft.log)}]=[{raft.curTerm},"{content}"]')
                    raft.log.append(content)
                    raft.nodeLog[len(raft.log)].add(raft.id)
                    
            
            elif command[0] == "RECEIVE":
                if command[2] == "RequestVotes":

                    raft.recvRequestVote(int(command[1]), int(command[3]), int(command[4])) # ID Term Index

                elif command[2] == "RequestVotesResponse":
                    raft.recvVote(int(command[1]), int(command[3]), (command[4] == "true"), int(command[5])) # ID Term ifReply Index

                elif command[2] == "AppendEntries":
                    if len(command) == 4:
                        # Heartbeat
                        raft.recvAppendEntries(int(command[1]), int(command[3]), 0, None, None)

                    elif len(command) == 6:
                        content = command[5][2:-2]
                        raft.recvAppendEntries(int(command[1]), int(command[3]), 1, int(command[4]), content) # ID Term Type Index Msg


                
                elif command[2] == "AppendEntriesResponse":
                    if len(command) == 6:
                        # raft.commitEntries(int(command[1]), int(command[4]), True) # ID Index
                        raft.commitEntries(int(command[1]), int(command[4]), command[5])
                    elif len(command) == 5:
                        # raft.commitEntries(int(command[1]), int(command[4]), False)
                        raft.commitEntries(int(command[1]), int(command[4]), None)
                    else:
                        raise IndexError("Append Error!")


            

        except asyncio.TimeoutError:
            raft.sendRequestVote()



    

















if __name__ == "__main__":
    # logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
    if len(sys.argv) != 3:
        raise IndexError("wrong command line format\n")

    try:
        nodeId, total = int(sys.argv[1]), int(sys.argv[2])
    except:
        raise TypeError("please input Integer\n")

    raft = Raft(nodeId, total)
    


    loop = asyncio.get_event_loop()

    loop.create_task(heartbeat())
    loop.create_task(raft_process())
    loop.run_until_complete(heartbeat())
    loop.run_until_complete(raft_process())
    loop.close()