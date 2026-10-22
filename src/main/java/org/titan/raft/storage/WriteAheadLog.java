package org.titan.raft.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

public final class WriteAheadLog implements AutoCloseable {

    private static final int BLOCK_SIZE = 4096;
    private static final int HEADER_SIZE = 24; // CRC32C (4) + Term (8) + Index (8) + Command Length (4)
    private static final int INITIAL_MAPPING_SIZE = 64 * 1024 * 1024; // 64 MB

    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private MappedByteBuffer mappedBuffer;
    
    private long fileSize;
    private long writePosition;
    private long lastLogIndex;
    private long lastLogTerm;

    private final CRC32C crc32c = new CRC32C();

    public WriteAheadLog(File walFile) throws IOException {
        this.file = walFile;
        boolean exists = file.exists();
        if (!exists) {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                if (!parent.mkdirs() && !parent.exists()) {
                    throw new IOException("Failed to create parent directories for WAL: " + parent.getAbsolutePath());
                }
            }
        }

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();
        this.fileSize = fileChannel.size();

        if (this.fileSize == 0) {
            this.fileSize = INITIAL_MAPPING_SIZE;
            fileChannel.truncate(this.fileSize);
        }

        this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
        this.writePosition = 0;
        this.lastLogIndex = 0;
        this.lastLogTerm = 0;

        recoverAndPosition();
    }

    private void recoverAndPosition() throws IOException {
        mappedBuffer.position(0);
        long validLimit = 0;
        long maxIndex = 0;
        long maxTerm = 0;

        while (mappedBuffer.remaining() >= HEADER_SIZE) {
            int currentPos = mappedBuffer.position();
            
            int checksum = mappedBuffer.getInt();
            long term = mappedBuffer.getLong();
            long index = mappedBuffer.getLong();
            int length = mappedBuffer.getInt();

            if (checksum == 0 && term == 0 && index == 0 && length == 0) {
                // Reached unwritten disk space (zero-filled)
                mappedBuffer.position(currentPos);
                break;
            }

            if (length < 0 || mappedBuffer.remaining() < length) {
                // Corrupted length or partial write boundary detected
                System.err.println("WAL corruption or partial write detected at position: " + currentPos + ". Truncating.");
                break;
            }

            byte[] payload = new byte[length];
            mappedBuffer.get(payload);

            crc32c.reset();
            crc32c.update((int)(term >>> 32));
            crc32c.update((int)term);
            crc32c.update((int)(index >>> 32));
            crc32c.update((int)index);
            crc32c.update(length);
            crc32c.update(payload, 0, length);
            int computedChecksum = (int) crc32c.getValue();

            if (computedChecksum != checksum) {
                System.err.println("WAL CRC32C mismatch at position: " + currentPos + ". Expected: " + checksum + ", Computed: " + computedChecksum + ". Truncating.");
                break;
            }

            validLimit = mappedBuffer.position();
            maxIndex = index;
            maxTerm = term;
        }

        this.writePosition = validLimit;
        this.lastLogIndex = maxIndex;
        this.lastLogTerm = maxTerm;

        // Truncate any trailing garbage beyond the valid recovery limit
        if (this.writePosition < this.fileSize) {
            // Zero out remaining mapped space from writePosition to ensure clean state
            int oldPos = mappedBuffer.position();
            mappedBuffer.position((int) this.writePosition);
            while (mappedBuffer.hasRemaining()) {
                mappedBuffer.put((byte) 0);
            }
            mappedBuffer.position(oldPos);
        }
        
        mappedBuffer.position((int) this.writePosition);
    }

    public synchronized void append(long term, long index, ByteBuffer command) throws IOException {
        int commandLength = command.remaining();
        int recordSize = HEADER_SIZE + commandLength;

        ensureCapacity(recordSize);

        int startPos = (int) writePosition;
        mappedBuffer.position(startPos);

        // Reserve space for checksum, write header fields first to compute CRC
        mappedBuffer.position(startPos + 4);
        mappedBuffer.putLong(term);
        mappedBuffer.putLong(index);
        mappedBuffer.putInt(commandLength);

        int payloadStartPos = mappedBuffer.position();
        int commandPos = command.position();
        mappedBuffer.put(command);
        command.position(commandPos); // Reset command buffer position

        // Compute CRC32C over [Term (8B)][Index (8B)][Command Length (4B)][Payload (Variable)]
        crc32c.reset();
        crc32c.update((int)(term >>> 32));
        crc32c.update((int)term);
        crc32c.update((int)(index >>> 32));
        crc32c.update((int)index);
        crc32c.update(commandLength);

        // Read back payload from mapped buffer to feed CRC32C precisely
        byte[] payloadVerification = new byte[commandLength];
        int currentBufPos = mappedBuffer.position();
        mappedBuffer.position(payloadStartPos);
        mappedBuffer.get(payloadVerification);
        mappedBuffer.position(currentBufPos);

        crc32c.update(payloadVerification, 0, commandLength);
        int checksum = (int) crc32c.getValue();

        // Write checksum back to the head of the record
        mappedBuffer.putInt(startPos, checksum);

        writePosition += recordSize;
        lastLogIndex = index;
        lastLogTerm = term;

        // Force flush to ensure durability according to Raft guarantees
        mappedBuffer.force();
    }

    private void ensureCapacity(int requiredSpace) throws IOException {
        if (writePosition + requiredSpace > fileSize) {
            long newSize = fileSize;
            while (writePosition + requiredSpace > newSize) {
                newSize *= 2; // Double the file size
            }

            // Align new size to OS block size (4KB)
            long remainder = newSize % BLOCK_SIZE;
            if (remainder != 0) {
                newSize += (BLOCK_SIZE - remainder);
            }

            // Unmap current buffer before resizing file channel
            mappedBuffer.force();
            fileChannel.truncate(newSize);
            this.fileSize = newSize;
            this.mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
            this.mappedBuffer.position((int) writePosition);
        }
    }

    public synchronized void truncate(long targetIndex) throws IOException {
        if (targetIndex >= lastLogIndex) {
            return; // Nothing to truncate
        }

        mappedBuffer.position(0);
        long newWritePos = 0;
        long newLastIndex = 0;
        long newLastTerm = 0;

        while (mappedBuffer.remaining() >= HEADER_SIZE) {
            int currentPos = mappedBuffer.position();
            
            int checksum = mappedBuffer.getInt();
            long term = mappedBuffer.getLong();
            long index = mappedBuffer.getLong();
            int length = mappedBuffer.getInt();

            if (checksum == 0 && term == 0 && index == 0 && length == 0 || index > targetIndex) {
                mappedBuffer.position(currentPos);
                break;
            }

            if (length < 0 || mappedBuffer.remaining() < length) {
                break;
            }

            mappedBuffer.position(mappedBuffer.position() + length);
            newWritePos = mappedBuffer.position();
            newLastIndex = index;
            newLastTerm = term;
        }

        // Zero out truncated region
        int truncateStart = (int) newWritePos;
        mappedBuffer.position(truncateStart);
        while (mappedBuffer.position() < writePosition) {
            mappedBuffer.put((byte) 0);
        }

        this.writePosition = newWritePos;
        this.lastLogIndex = newLastIndex;
        this.lastLogTerm = newLastTerm;
        mappedBuffer.position(truncateStart);
        mappedBuffer.force();
    }

    public synchronized LogEntry readEntry(long index) throws IOException {
        mappedBuffer.position(0);

        while (mappedBuffer.remaining() >= HEADER_SIZE) {
            int checksum = mappedBuffer.getInt();
            long term = mappedBuffer.getLong();
            long curIndex = mappedBuffer.getLong();
            int length = mappedBuffer.getInt();

            if (checksum == 0 && term == 0 && curIndex == 0 && length == 0) {
                break;
            }

            if (length < 0 || mappedBuffer.remaining() < length) {
                break;
            }

            ByteBuffer payload = ByteBuffer.allocate(length);
            mappedBuffer.get(payload.array(), 0, length);

            if (curIndex == index) {
                payload.position(0);
                return new LogEntry(term, curIndex, payload);
            }
        }

        return null;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public synchronized void close() throws IOException {
        if (mappedBuffer != null) {
            mappedBuffer.force();
            // MappedByteBuffer unmapping relies on GC, but we can close the underlying channel
        }
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
        if (randomAccessFile != null) {
            randomAccessFile.close();
        }
    }

    public static final class LogEntry {
        private final long term;
        private final long index;
        private final ByteBuffer command;

        public LogEntry(long term, long index, ByteBuffer command) {
            this.term = term;
            this.index = index;
            this.command = command;
        }

        public long getTerm() {
            return term;
        }

        public long getIndex() {
            return index;
        }

        public ByteBuffer getCommand() {
            return command;
        }
    }
}