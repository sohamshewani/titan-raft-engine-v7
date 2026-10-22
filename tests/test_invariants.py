package com.titan.raft.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.concurrent.Execution;
import org.junit.jupiter.api.concurrent.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit, invariant, and concurrent stress-test suite for 'titan-raft-engine'.
 * Contains embedded implementations of the Raft core components to provide a fully self-contained,
 * runnable, enterprise-grade validation suite.
 */
@Execution(ExecutionMode.CONCURRENT)
public class TitanRaftEngineTestSuite {

    // =========================================================================
    // EMBEDDED RAFT ENGINE COMPONENTS (titan-raft-engine core simulation)
    // =========================================================================

    public enum NodeState {
        FOLLOWER, CANDIDATE, LEADER
    }

    public static class LogEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        private final long index;
        private final long term;
        private final byte[] command;

        public LogEntry(long index, long term, byte[] command) {
            this.index = index;
            this.term = term;
            this.command = command;
        }

        public long getIndex() { return index; }
        public long getTerm() { return term; }
        public byte[] getCommand() { return command; }

        public byte[] serialize() {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeLong(index);
                dos.writeLong(term);
                dos.writeInt(command != null ? command.length : 0);
                if (command != null) {
                    dos.write(command);
                }
                return baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static LogEntry deserialize(DataInputStream dis) throws IOException {
            long idx = dis.readLong();
            long trm = dis.readLong();
            int len = dis.readInt();
            byte[] cmd = new byte[len];
            dis.readFully(cmd);
            return new LogEntry(idx, termVal(trm), cmd);
        }

        private static long termVal(long t) { return t; }
    }

    public interface RaftNetwork {
        CompletableFuture<AppendEntriesResponse> sendAppendEntries(String targetNode, AppendEntriesRequest request);
        CompletableFuture<RequestVoteResponse> sendRequestVote(String targetNode, RequestVoteRequest request);
    }

    public static class AppendEntriesRequest {
        public long term;
        public String leaderId;
        public long prevLogIndex;
        public long prevLogTerm;
        public List<LogEntry> entries;
        public long leaderCommit;
    }

    public static class AppendEntriesResponse {
        public long term;
        public boolean success;
        public long matchIndex;
    }

    public static class RequestVoteRequest {
        public long term;
        public String candidateId;
        public long lastLogIndex;
        public long lastLogTerm;
    }

    public static class RequestVoteResponse {
        public long term;
        public boolean voteGranted;
    }

    public interface StateMachine {
        byte[] apply(byte[] command);
        void snapshot(OutputStream out) throws IOException;
        void restore(InputStream in) throws IOException;
    }

    public static class RaftLog {
        private final List<LogEntry> entries = new ArrayList<>();
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private long lastSnapshotIndex = 0;
        private long lastSnapshotTerm = 0;

        public RaftLog() {
            // Sentinel entry at index 0
            entries.add(new LogEntry(0, 0, new byte[0]));
        }

        public void append(LogEntry entry) {
            lock.writeLock().lock();
            try {
                entries.add(entry);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public LogEntry getEntry(long index) {
            lock.readLock().lock();
            try {
                long internalIndex = index - lastSnapshotIndex;
                if (internalIndex < 0 || internalIndex >= entries.size()) {
                    return null;
                }
                return entries.get((int) internalIndex);
            } finally {
                lock.readLock().unlock();
            }
        }

        public long getLastIndex() {
            lock.readLock().lock();
            try {
                if (entries.isEmpty()) return lastSnapshotIndex;
                return entries.get(entries.size() - 1).getIndex();
            } finally {
                lock.readLock().unlock();
            }
        }

        public long getLastTerm() {
            lock.readLock().lock();
            try {
                if (entries.isEmpty()) return lastSnapshotTerm;
                return entries.get(entries.size() - 1).getTerm();
            } finally {
                lock.readLock().unlock();
            }
        }

        public void truncateFrom(long index) {
            lock.writeLock().lock();
            try {
                long internalIndex = index - lastSnapshotIndex;
                if (internalIndex > 0 && internalIndex < entries.size()) {
                    entries.subList((int) internalIndex, entries.size()).clear();
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        public List<LogEntry> getEntriesFrom(long index) {
            lock.readLock().lock();
            try {
                List<LogEntry> sub = new ArrayList<>();
                for (LogEntry entry : entries) {
                    if (entry.getIndex() >= index) {
                        sub.add(entry);
                    }
                }
                return sub;
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    public static class RaftNode implements AutoCloseable {
        private final String nodeId;
        private final List<String> clusterPeers;
        private final RaftNetwork network;
        private final StateMachine stateMachine;
        private final Path storagePath;

        private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
        private NodeState state = NodeState.FOLLOWER;
        private long currentTerm = 0;
        private String votedFor = null;

        private final RaftLog raftLog = new RaftLog();
        private long commitIndex = 0;
        private long lastApplied = 0;

        // Leader state
        private final ConcurrentMap<String, Long> nextIndex = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Long> matchIndex = new ConcurrentHashMap<>();

        private volatile boolean running = true;
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        private ScheduledFuture<?> electionTimer;
        private ScheduledFuture<?> heartbeatTimer;
        private final Random random = new Random();

        public RaftNode(String nodeId, List<String> clusterPeers, RaftNetwork network, StateMachine stateMachine, Path storagePath) {
            this.nodeId = nodeId;
            this.clusterPeers = clusterPeers;
            this.network = network;
            this.stateMachine = stateMachine;
            this.storagePath = storagePath;
            resetElectionTimeout();
        }

        public void start() {
            stateLock.writeLock().lock();
            try {
                resetElectionTimeoutUnsafe();
            } finally {
                stateLock.writeLock().unlock();
            }
        }

        private void resetElectionTimeout() {
            stateLock.writeLock().lock();
            try {
                resetElectionTimeoutUnsafe();
            } finally {
                stateLock.writeLock().unlock();
            }
        }

        private void resetElectionTimeoutUnsafe() {
            if (electionTimer != null) electionTimer.cancel(true);
            int timeout = 150 + random.nextInt(150);
            electionTimer = scheduler.schedule(() -> {
                try {
                    handleElectionTimeout();
                } catch (Exception ignored) {}
            }, timeout, TimeUnit.MILLISECONDS);
        }

        private void handleElectionTimeout() {
            stateLock.writeLock().lock();
            try {
                if (!running || state == NodeState.LEADER) return;
                state = NodeState.CANDIDATE;
                currentTerm++;
                votedFor = nodeId;
                resetElectionTimeoutUnsafe();

                long termToRequest = currentTerm;
                long lastIdx = raftLog.getLastIndex();
                long lastTerm = raftLog.getLastTerm();

                stateLock.writeLock().unlock();
                try {
                    AtomicInteger votes = new AtomicInteger(1);
                    int majority = (clusterPeers.size() + 1) / 2 + 1;

                    for (String peer : clusterPeers) {
                        RequestVoteRequest req = new RequestVoteRequest();
                        req.term = termToRequest;
                        req.candidateId = nodeId;
                        req.lastLogIndex = lastIdx;
                        req.lastLogTerm = lastTerm;

                        network.sendRequestVote(peer, req).thenAccept(res -> {
                            if (res == null) return;
                            stateLock.writeLock().lock();
                            try {
                                if (state == NodeState.CANDIDATE && currentTerm == termToRequest) {
                                    if (res.term > currentTerm) {
                                        becomeFollower(res.term);
                                        return;
                                    }
                                    if (res.voteGranted && votes.incrementAndGet() >= majority) {
                                        becomeLeader();
                                    }
                                } else if (res.term > currentTerm) {
                                    becomeFollower(res.term);
                                }
                            } finally {
                                stateLock.writeLock().unlock();
                            }
                        });
                    }
                } finally {
                    stateLock.writeLock().lock(); // Re-acquire to satisfy lock balance in finally if not careful, handled cleanly below
                }
            } finally {
                if (stateLock.isWriteLockedByCurrentThread()) {
                    stateLock.writeLock().unlock();
                }
            }
        }

        private void becomeLeader() {
            state = NodeState.LEADER;
            if (electionTimer != null) electionTimer.cancel(true);
            for (String peer : clusterPeers) {
                nextIndex.put(peer, raftLog.getLastIndex() + 1);
                matchIndex.put(peer, 0L);
            }
            startHeartbeatTimer();
            sendHeartbeats();
        }

        private void becomeFollower(long term) {
            state = NodeState.FOLLOWER;
            currentTerm = term;
            votedFor = null;
            if (heartbeatTimer != null) heartbeatTimer.cancel(true);
            resetElectionTimeoutUnsafe();
        }

        private void startHeartbeatTimer() {
            if (heartbeatTimer != null) heartbeatTimer.cancel(true);
            heartbeatTimer = scheduler.scheduleAtFixedRate(this::sendHeartbeats, 0, 50, TimeUnit.MILLISECONDS);
        }

        private void sendHeartbeats() {
            stateLock.readLock().lock();
            try {
                if (state != NodeState.LEADER || !running) return;
                long t = currentTerm;
                long commit = commitIndex;
                String leader = nodeId;

                for (String peer : clusterPeers) {
                    long prevIdx = nextIndex.getOrDefault(peer, 1L) - 1;
                    LogEntry prevEntry = raftLog.getEntry(prevIdx);
                    long prevTerm = prevEntry != null ? prevEntry.getTerm() : 0;
                    List<LogEntry> entries = raftLog.getEntriesFrom(prevIdx + 1);

                    AppendEntriesRequest req = new AppendEntriesRequest();
                    req.term = t;
                    req.leaderId = leader;
                    req.prevLogIndex = prevIdx;
                    req.prevLogTerm = prevTerm;
                    req.entries = entries;
                    req.leaderCommit = commit;

                    network.sendAppendEntries(peer, req).thenAccept(res -> {
                        if (res == null) return;
                        stateLock.writeLock().lock();
                        try {
                            if (