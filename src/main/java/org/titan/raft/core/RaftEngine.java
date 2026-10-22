package org.titan.raft.core;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.zip.CRC32C;

public final class RaftEngine {

    // --- System Constants & Layout Specifications ---
    private static final int PAGE_SIZE = 4096;
    private static final int INITIAL_LOG_CAPACITY = 131072; // 128K entries
    private static final int HEADER_SIZE = 24; // CRC32C (4) + Term (8) + Index (8) + Length (4)
    private static final int DIRECT_BUFFER_CAPACITY = 65536; // 64KB Arena buffer

    // --- Primitive Memory Layout Arrays (Zero-GC Design) ---
    private long[] logTerms;
    private long[] logIndexes;
    private long[] logOffsets; // Byte offset in WAL file for zero-copy seeks
    private int logSize;

    // --- Volatile Consensus State & VarHandles ---
    private volatile long currentTerm;
    private volatile long votedFor;
    private volatile long commitIndex;
    private volatile long lastApplied;

    private static final VarHandle CURRENT_TERM_HANDLE;
    private static final VarHandle VOTED_FOR_HANDLE;
    private static final VarHandle COMMIT_INDEX_HANDLE;
    private static final VarHandle LAST_APPLIED_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            CURRENT_TERM_HANDLE = l.findVarHandle(RaftEngine.class, "currentTerm", long.class);
            VOTED_FOR_HANDLE = l.findVarHandle(RaftEngine.class, "votedFor", long.class);
            COMMIT_INDEX_HANDLE = l.findVarHandle(RaftEngine.class, "commitIndex", long.class);
            LAST_APPLIED_HANDLE = l.findVarHandle(RaftEngine.class, "lastApplied", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // --- WAL & Off-Heap Arena Infrastructure ---
    private final File walFile;
    private FileChannel walChannel;
    private MappedByteBuffer walBuffer;
    private long walWriteOffset;
    private final ByteBuffer arenaBuffer;
    private final CRC32C crc32c;

    // --- Cluster Role & Node Identity ---
    public enum Role {
        FOLLOWER, CANDIDATE, LEADER
    }

    private volatile Role currentRole;
    private final long nodeId;
    private long leaderId;

    public RaftEngine(File walFile, long nodeId) {
        this.walFile = walFile;
        this.nodeId = nodeId;
        this.currentRole = Role.FOLLOWER;
        this.leaderId = -1L;

        this.logTerms = new long[INITIAL_LOG_CAPACITY];
        this.logIndexes = new long[INITIAL_LOG_CAPACITY];
        this.logOffsets = new long[INITIAL_LOG_CAPACITY];
        this.logSize = 0;

        this.currentTerm = 0L;
        this.votedFor = -1L;
        this.commitIndex = 0L;
        this.lastApplied = 0L;

        this.arenaBuffer = ByteBuffer.allocateDirect(DIRECT_BUFFER_CAPACITY).order(ByteOrder.LITTLE_ENDIAN);
        this.crc32c = new CRC32C();

        initializeStorageAndRecovery();
    }

    private void initializeStorageAndRecovery() {
        try {
            boolean fileExists = walFile.exists();
            if (!fileExists) {
                File parent = walFile.getParentFile();
                if (parent != null && !parent.exists()) {
                    parent.mkdirs();
                }
                walFile.createNewFile();
            }

            this.walChannel = FileChannel.open(
                    walFile.toPath(),
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE
            );

            long fileSize = walChannel.size();
            // Align file size to page boundary if needed, or map initial segment
            long mapSize = Math.max(PAGE_SIZE, (fileSize + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE);
            if (mapSize < fileSize) {
                mapSize = ((fileSize + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
            }
            
            // Fallback for empty or small files
            if (mapSize < 1024 * 1024) {
                mapSize = 16 * 1024 * 1024; // 16MB initial mapped block
            }

            this.walBuffer = walChannel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
            this.walBuffer.order(ByteOrder.LITTLE_ENDIAN);

            if (fileExists && fileSize > 0) {
                performCrashRecovery(fileSize);
            } else {
                this.walWriteOffset = 0L;
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RaftEngine storage and recovery", e);
        }
    }

    private void performCrashRecovery(long fileSize) {
        long scanOffset = 0L;
        long validLength = 0L;

        walBuffer.position(0);

        while (scanOffset + HEADER_SIZE <= fileSize) {
            walBuffer.position((int) scanOffset);
            int storedChecksum = walBuffer.getInt();
            long term = walBuffer.getLong();
            long index = walBuffer.getLong();
            int commandLength = walBuffer.getInt();

            long totalRecordSize = HEADER_SIZE + commandLength;
            if (scanOffset + totalRecordSize > fileSize) {
                // Partial write detected at tail, truncate here
                break;
            }

            // Read payload and verify CRC32C
            byte[] payload = new byte[commandLength];
            walBuffer.get(payload);

            crc32c.reset();
            crc32c.update(walBuffer.array(), walBuffer.arrayOffset() + (int) scanOffset + 4, (int) (totalRecordSize - 4));
            int computedChecksum = (int) crc32c.getValue();

            if (computedChecksum != storedChecksum) {
                // Corruption detected, truncate at last valid boundary
                break;
            }

            // Append to in-memory primitive structures
            ensureCapacity();
            logTerms[logSize] = term;
            logIndexes[logSize] = index;
            logOffsets[logSize] = scanOffset;
            logSize++;

            scanOffset += totalRecordSize;
            validLength = scanOffset;
        }

        this.walWriteOffset = validLength;
        walBuffer.position((int) validLength);
    }

    private void ensureCapacity() {
        if (logSize >= logTerms.length) {
            int newCapacity = logTerms.length * 2;
            long[] newTerms = new long[newCapacity];
            long[] newIndexes = new long[newCapacity];
            long[] newOffsets = new long[newCapacity];

            System.arraycopy(logTerms, 0, newTerms, 0, logSize);
            System.arraycopy(logIndexes, 0, newIndexes, 0, logSize);
            System.arraycopy(logOffsets, 0, newOffsets, 0, logSize);

            logTerms = newTerms;
            logIndexes = newIndexes;
            logOffsets = newOffsets;
        }
    }

    public synchronized void appendLogEntry(long term, byte[] command) {
        long index = logSize == 0 ? 1 : logIndexes[logSize - 1] + 1;
        int commandLength = command.length;
        int totalRecordSize = HEADER_SIZE + commandLength;

        // Check if MappedByteBuffer needs expansion
        long requiredCapacity = walWriteOffset + totalRecordSize;
        if (requiredCapacity > walBuffer.capacity()) {
            try {
                long newCapacity = walBuffer.capacity() * 2;
                while (newCapacity < requiredCapacity) {
                    newCapacity *= 2;
                }
                long currentPos = walWriteOffset;
                walBuffer = walChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
                walBuffer.order(ByteOrder.LITTLE_ENDIAN);
                walBuffer.position((int) currentPos);
            } catch (Exception e) {
                throw new RuntimeException("Failed to expand WAL memory-mapped file", e);
            }
        }

        // Calculate CRC32C over [Term (8B)][Index (8B)][Command Length (4B)][Payload (Variable)]
        arenaBuffer.clear();
        if (arenaBuffer.capacity() < 20 + commandLength) {
            throw new IllegalArgumentException("Command payload exceeds off-heap arena buffer capacity");
        }
        arenaBuffer.putLong(term);
        arenaBuffer.putLong(index);
        arenaBuffer.putInt(commandLength);
        arenaBuffer.put(command);

        crc32c.reset();
        crc32c.update(arenaBuffer.array(), arenaBuffer.arrayOffset(), 20 + commandLength);
        int checksum = (int) crc32c.getValue();

        // Write directly to MappedByteBuffer
        int startOffset = (int) walWriteOffset;
        walBuffer.position(startOffset);
        walBuffer.putInt(checksum);
        walBuffer.putLong(term);
        walBuffer.putLong(index);
        walBuffer.putInt(commandLength);
        walBuffer.put(command);

        // Force flush page alignment for durability guarantees
        walBuffer.force();

        ensureCapacity();
        logTerms[logSize] = term;
        logIndexes[logSize] = index;
        logOffsets[logSize] = startOffset;
        logSize++;

        walWriteOffset += totalRecordSize;
    }

    public boolean handleRequestVote(long candidateTerm, long candidateId, long lastLogIndex, long lastLogTerm) {
        long currentT = (long) CURRENT_TERM_HANDLE.getAcquire(this);

        if (candidateTerm < currentT) {
            return false;
        }

        if (candidateTerm > currentT) {
            stepDown(candidateTerm);
        }

        long votedF = (long) VOTED_FOR_HANDLE.getAcquire(this);
        boolean termMatch = (votedF == -1L || votedF == candidateId);

        // Log completeness check (Raft §5.4.1)
        long localLastTerm = logSize == 0 ? 0L : logTerms[logSize - 1];
        long localLastIndex = logSize == 0 ? 0L : logIndexes[logSize - 1];

        boolean logIsUpToDate = (lastLogTerm > localLastTerm) ||
                (lastLogTerm == localLastTerm && lastLogIndex >= localLastIndex);

        if (termMatch && logIsUpToDate) {
            VOTED_FOR_HANDLE.setRelease(this, candidateId);
            return true;
        }

        return false;
    }

    public synchronized void stepDown(long newTerm) {
        CURRENT_TERM_HANDLE.setRelease(this, newTerm);
        VOTED_FOR_HANDLE.setRelease(this, -1L);
        this.currentRole = Role.FOLLOWER;
        this.leaderId = -1L;
    }

    public boolean handleAppendEntries(long leaderTerm, long leaderId, long prevLogIndex, long prevLogTerm,
                                       long leaderCommit, byte[][] entriesTerms, long[] entriesIndexes, byte[][] entriesCommands) {
        long currentT = (long) CURRENT_TERM_HANDLE.getAcquire(this);

        if (leaderTerm < currentT) {
            return false;
        }

        if (leaderTerm > currentT || currentRole == Role.CANDIDATE) {
            stepDown(leaderTerm);
        }

        this.leaderId = leaderId;

        // Log Matching Invariant Check (§5.3)
        if (prevLogIndex > 0) {
            int localIndexPos = findLogIndexPosition(prevLogIndex);
            if (localIndexPos == -1) {
                return false;
            }
            if (logTerms[localIndexPos] != prevLogTerm) {
                return false;
            }
        }

        // Process incoming entries and resolve conflicts
        for (int i = 0; i < entriesIndexes.length; i++) {
            long entryIndex = entriesIndexes[i];
            long entryTerm = entriesTermsBytesToLong(entriesTerms[i]); // or direct term array
            byte[] entryCommand = entriesCommands[i];

            int existingPos = findLogIndexPosition(entryIndex);
            if (existingPos != -1) {
                if (logTerms[existingPos] != entryTerm) {
                    // Truncate conflicting conflicting logs from this index onwards
                    truncateLog(existingPos);
                    appendLogEntry(entryTerm, entryCommand);
                }
                // If term matches, entry already exists; invariant guarantees identical content.
            } else {
                appendLogEntry(entryTerm, entryCommand);
            }
        }

        // Update commit index
        if (leaderCommit > (long) COMMIT_INDEX_HANDLE.getAcquire(this)) {
            long lastNewEntryIndex = logSize == 0 ? 0L : logIndexes[logSize - 1];
            long newCommitIndex = Math.min(leaderCommit, lastNewEntryIndex);
            COMMIT_INDEX_HANDLE.setRelease(this, newCommitIndex);
            applyEntriesToStateMachine();
        }

        return true;
    }

    private long entriesTermsBytesToLong(byte[] termBytes) {
        ByteBuffer bb = ByteBuffer.wrap(termBytes).order(ByteOrder.LITTLE_ENDIAN);
        return bb.getLong();
    }

    private int findLogIndexPosition(long index) {
        // Binary search over tight primitive array
        int low = 0;
        int high = logSize - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = logIndexes[mid];

            if (midVal == index) {
                return mid;
            } else if (midVal < index) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return -1;
    }

    private void truncateLog(int fromPosition) {
        this.logSize = fromPosition;
        if (logSize > 0) {
            this.walWriteOffset = logOffsets[logSize - 1] + HEADER_SIZE + getCommandLengthAt(logOffsets[logSize - 1]);
        } else {
            this.walWriteOffset = 0L;
        }
        walBuffer.position((int) walWriteOffset);
    }

    private int getCommandLengthAt(long offset) {
        int pos = walBuffer.position();
        walBuffer.position((int) offset + 16); // Skip CRC(4) + Term(8) + Index(8)
        int len = walBuffer.getInt();
        walBuffer.position(pos);
        return len;
    }

    private void applyEntriesToStateMachine() {
        long commitIdx = (long) COMMIT_INDEX_HANDLE.getAcquire(this);
        long appliedIdx = (long) LAST_APPLIED_HANDLE.getAcquire(this);

        while (commitIdx > appliedIdx) {
            appliedIdx++;
            int pos = findLogIndexPosition(appliedIdx);
            if (pos != -1) {
                // Execute command against state machine (State Machine Safety Invariant)
                executeStateMachineCommand(pos);
            }
            LAST_APPLIED_HANDLE.setRelease(this, appliedIdx);
        }
    }

    private void executeStateMachineCommand(int logPosition) {
        long offset = logOffsets[logPosition];
        int commandLen = getCommandLengthAt(offset);
        
        // Zero-copy off-heap extraction point for state machine executor
        walBuffer.position((int) offset + HEADER_SIZE);
        arenaBuffer.clear();
        arenaBuffer.limit(commandLen);
        walBuffer.get(arenaBuffer.array(), arenaBuffer.arrayOffset(), commandLen);

        // State Machine hook execution (Deterministic Application)
    }

    public void close() {
        try {
            if (walChannel != null) {
                walChannel.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to cleanly shutdown RaftEngine storage", e);
        }