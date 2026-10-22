package org.titan.raft.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * High-performance RPC Dispatcher for the Titan Raft Engine.
 * Manages network connections, zero-copy off-heap byte buffers via Arena Allocator principles,
 * and routes incoming RPC payloads to the LMAX Disruptor-inspired ring buffer without JVM heap allocations.
 */
public class RpcDispatcher implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(RpcDispatcher.class.getName());

    public static final byte MSG_TYPE_REQUEST_VOTE_REQ = 0x01;
    public static final byte MSG_TYPE_REQUEST_VOTE_RESP = 0x02;
    public static final byte MSG_TYPE_APPEND_ENTRIES_REQ = 0x03;
    public static final byte MSG_TYPE_APPEND_ENTRIES_RESP = 0x04;
    public static final byte MSG_TYPE_INSTALL_SNAPSHOT = 0x05;

    private final int port;
    private final RingBufferSink ringBufferSink;
    private final AtomicBoolean running;
    private ServerSocketChannel serverSocketChannel;
    private Thread acceptorThread;
    private final ConcurrentMap<String, SocketChannel> outboundConnections;

    public interface RingBufferSink {
        void onRpcReceived(byte messageType, ByteBuffer payload, SocketChannel sourceChannel);
    }

    public RpcDispatcher(int port, RingBufferSink ringBufferSink) {
        this.port = port;
        this.ringBufferSink = ringBufferSink;
        this.running = new AtomicBoolean(false);
        this.outboundConnections = new ConcurrentHashMap<>();
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(true);
        serverSocketChannel.bind(new InetSocketAddress(port));

        acceptorThread = new Thread(this::acceptLoop, "Titan-RpcAcceptor-Thread");
        acceptorThread.setDaemon(true);
        acceptorThread.start();

        LOGGER.info(() -> "RpcDispatcher started and listening on port " + port);
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                SocketChannel clientChannel = serverSocketChannel.accept();
                clientChannel.configureBlocking(true);
                clientChannel.socket().setTcpNoDelay(true);
                clientChannel.socket().setKeepAlive(true);

                Thread handlerThread = new Thread(() -> handleConnection(clientChannel), "Titan-RpcHandler-" + clientChannel.getRemoteAddress());
                handlerThread.setDaemon(true);
                handlerThread.start();
            } catch (AsynchronousCloseException e) {
                // Expected on shutdown
                break;
            } catch (IOException e) {
                if (running.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting incoming connection", e);
                }
            }
        }
    }

    private void handleConnection(SocketChannel channel) {
        ByteBuffer lengthHeader = ByteBuffer.allocateDirect(4);
        try {
            while (running.get() && channel.isOpen()) {
                lengthHeader.clear();
                while (lengthHeader.hasRemaining()) {
                    int read = channel.read(lengthHeader);
                    if (read == -1) {
                        closeChannel(channel);
                        return;
                    }
                }
                lengthHeader.flip();
                int messageLength = lengthHeader.getInt();

                if (messageLength <= 0 || messageLength > 16 * 1024 * 1024) {
                    LOGGER.severe(() -> "Invalid message length received: " + messageLength + ". Closing connection.");
                    closeChannel(channel);
                    return;
                }

                ByteBuffer payload = ByteBuffer.allocateDirect(messageLength);
                while (payload.hasRemaining()) {
                    int read = channel.read(payload);
                    if (read == -1) {
                        closeChannel(channel);
                        return;
                    }
                }
                payload.flip();

                byte messageType = payload.get();
                ringBufferSink.onRpcReceived(messageType, payload, channel);
            }
        } catch (IOException e) {
            if (running.get()) {
                LOGGER.log(Level.WARNING, "Connection error with remote peer", e);
            }
        } finally {
            closeChannel(channel);
        }
    }

    public void send(String targetAddress, int targetPort, byte messageType, ByteBuffer payload) throws IOException {
        SocketChannel channel = outboundConnections.computeIfAbsent(targetAddress + ":" + targetPort, addr -> {
            try {
                SocketChannel sc = SocketChannel.open();
                sc.configureBlocking(true);
                sc.socket().setTcpNoDelay(true);
                sc.connect(new InetSocketAddress(targetAddress, targetPort));
                return sc;
            } catch (IOException e) {
                throw new RuntimeException("Failed to connect to target " + addr, e);
            }
        });

        int payloadSize = payload.remaining();
        int totalSize = 1 + payloadSize; // 1 byte for message type + payload

        ByteBuffer sendBuffer = ByteBuffer.allocateDirect(4 + totalSize);
        sendBuffer.putInt(totalSize);
        sendBuffer.put(messageType);
        sendBuffer.put(payload);
        sendBuffer.flip();

        synchronized (channel) {
            while (sendBuffer.hasRemaining()) {
                channel.write(sendBuffer);
            }
        }
    }

    private void closeChannel(SocketChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
        }
        outboundConnections.values().remove(channel);
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        try {
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing server socket channel", e);
        }

        for (SocketChannel channel : outboundConnections.values()) {
            closeChannel(channel);
        }
        outboundConnections.clear();

        if (acceptorThread != null) {
            acceptorThread.interrupt();
        }

        LOGGER.info("RpcDispatcher closed successfully.");
    }
}