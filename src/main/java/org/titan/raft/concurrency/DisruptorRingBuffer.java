package org.titan.raft.concurrency;

import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

public final class DisruptorRingBuffer<T> {
    private final int bufferSize;
    private final int indexMask;
    private final Object[] entries;
    private final long[] sequence;
    private final Sequencer sequencer;

    public DisruptorRingBuffer(int bufferSize, EventFactory<T> eventFactory, WaitStrategy waitStrategy) {
        if (bufferSize <= 0 || (bufferSize & (bufferSize - 1)) != 0) {
            throw new IllegalArgumentException("bufferSize must be a power of 2 and greater than 0");
        }
        this.bufferSize = bufferSize;
        this.indexMask = bufferSize - 1;
        this.entries = new Object[bufferSize];
        for (int i = 0; i < bufferSize; i++) {
            this.entries[i] = eventFactory.newInstance();
        }
        this.sequence = new long[32]; // Padding cache line separation simulation
        this.sequence[16] = -1L; // cursor initialized to -1
        this.sequencer = new Sequencer(bufferSize, waitStrategy);
    }

    public long next() {
        return sequencer.next();
    }

    public long next(int n) {
        return sequencer.next(n);
    }

    @SuppressWarnings("unchecked")
    public T get(long sequence) {
        return (T) entries[(int) sequence & indexMask];
    }

    public void publish(long sequence) {
        sequencer.publish(sequence);
    }

    public void publish(long lo, long hi) {
        sequencer.publish(lo, hi);
    }

    public long getCursor() {
        return sequencer.getCursor();
    }

    public boolean hasAvailableCapacity(int requiredCapacity) {
        return sequencer.hasAvailableCapacity(requiredCapacity);
    }

    public long remainingCapacity() {
        return sequencer.remainingCapacity(bufferSize);
    }

    public void addGatingSequences(Sequence... gatingSequences) {
        sequencer.addGatingSequences(gatingSequences);
    }

    public boolean removeGatingSequence(Sequence gatingSequence) {
        return sequencer.removeGatingSequence(gatingSequence);
    }

    public EventProducer<T> createProducer() {
        return new EventProducer<>(this);
    }

    public static final class Sequencer {
        private final int bufferSize;
        private final WaitStrategy waitStrategy;
        private volatile long cursor = -1L;
        private volatile Sequence[] gatingSequences = new Sequence[0];

        public Sequencer(int bufferSize, WaitStrategy waitStrategy) {
            this.bufferSize = bufferSize;
            this.waitStrategy = waitStrategy;
        }

        public long getCursor() {
            return cursor;
        }

        public long next() {
            return next(1);
        }

        public long next(int n) {
            if (n < 1) {
                throw new IllegalArgumentException("n must be >= 1");
            }
            long nextSequence;
            long wrappedSequence;
            do {
                nextSequence = cursor + n;
                wrappedSequence = nextSequence - bufferSize;
                long cachedGatingSequence = getMinimumSequence(gatingSequences);
                if (wrappedSequence > cachedGatingSequence) {
                    LockSupport.parkNanos(1); // Backpressure yield
                    continue;
                }
                break;
            } while (true);

            cursor = nextSequence;
            return nextSequence;
        }

        public void publish(long sequence) {
            cursor = sequence;
            waitStrategy.signalAllWhenBlocking();
        }

        public void publish(long lo, long hi) {
            cursor = hi;
            waitStrategy.signalAllWhenBlocking();
        }

        public boolean hasAvailableCapacity(int requiredCapacity) {
            long nextSequence = cursor + requiredCapacity;
            long wrappedSequence = nextSequence - bufferSize;
            return wrappedSequence <= getMinimumSequence(gatingSequences);
        }

        public long remainingCapacity(int bufferSize) {
            long consumed = getMinimumSequence(gatingSequences);
            long produced = cursor;
            long computed = produced - consumed;
            if (computed < 0) {
                return bufferSize;
            }
            return bufferSize - computed;
        }

        public synchronized void addGatingSequences(Sequence... sequencesToAdd) {
            Objects.requireNonNull(sequencesToAdd, "sequencesToAdd cannot be null");
            Sequence[] current = gatingSequences;
            Sequence[] updated = new Sequence[current.length + sequencesToAdd.length];
            System.arraycopy(current, 0, updated, 0, current.length);
            System.arraycopy(sequencesToAdd, 0, updated, current.length, sequencesToAdd.length);
            gatingSequences = updated;
        }

        public synchronized boolean removeGatingSequence(Sequence sequenceToRemove) {
            Sequence[] current = gatingSequences;
            int foundIndex = -1;
            for (int i = 0; i < current.length; i++) {
                if (current[i].equals(sequenceToRemove)) {
                    foundIndex = i;
                    break;
                }
            }
            if (foundIndex < 0) {
                return false;
            }
            Sequence[] updated = new Sequence[current.length - 1];
            System.arraycopy(current, 0, updated, 0, foundIndex);
            System.arraycopy(current, foundIndex + 1, updated, foundIndex, current.length - foundIndex - 1);
            gatingSequences = updated;
            return true;
        }

        private static long getMinimumSequence(Sequence[] sequences) {
            long minimum = Long.MAX_VALUE;
            for (Sequence seq : sequences) {
                long value = seq.get();
                if (value < minimum) {
                    minimum = value;
                }
            }
            return minimum == Long.MAX_VALUE ? -1L : minimum;
        }
    }

    public static final class Sequence {
        private volatile long value;

        public Sequence(long initialValue) {
            this.value = initialValue;
        }

        public long get() {
            return value;
        }

        public void set(long value) {
            this.value = value;
        }

        public void setVolatile(long value) {
            this.value = value;
        }

        public boolean compareAndSet(long expectedValue, long newValue) {
            if (this.value == expectedValue) {
                this.value = newValue;
                return true;
            }
            return false;
        }

        public long incrementAndGet() {
            return ++value;
        }

        public long addAndGet(long delta) {
            return value += delta;
        }
    }

    public interface WaitStrategy {
        long waitFor(long sequence, Sequence cursor, Sequence gatingSequence, SequenceBarrier barrier) throws InterruptedException;
        void signalAllWhenBlocking();
    }

    public static final class BusySpinWaitStrategy implements WaitStrategy {
        @Override
        public long waitFor(long sequence, Sequence cursor, Sequence gatingSequence, SequenceBarrier barrier) throws InterruptedException {
            long availableSequence;
            while ((availableSequence = cursor.get()) < sequence) {
                barrier.checkAlert();
                Thread.onSpinWait();
            }
            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking() {
        }
    }

    public static final class BlockingWaitStrategy implements WaitStrategy {
        private final Object mutex = new Object();

        @Override
        public long waitFor(long sequence, Sequence cursor, Sequence gatingSequence, SequenceBarrier barrier) throws InterruptedException {
            long availableSequence;
            if ((availableSequence = cursor.get()) < sequence) {
                synchronized (mutex) {
                    while ((availableSequence = cursor.get()) < sequence) {
                        barrier.checkAlert();
                        mutex.wait();
                    }
                }
            }
            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking() {
            synchronized (mutex) {
                mutex.notifyAll();
            }
        }
    }

    public static final class YieldingWaitStrategy implements WaitStrategy {
        private static final int SPINS = 100;

        @Override
        public long waitFor(long sequence, Sequence cursor, Sequence gatingSequence, SequenceBarrier barrier) throws InterruptedException {
            long availableSequence;
            int counter = SPINS;
            while ((availableSequence = cursor.get()) < sequence) {
                barrier.checkAlert();
                counter = applyWaitMethod(counter);
            }
            return availableSequence;
        }

        @Override
        public void signalAllWhenBlocking() {
        }

        private int applyWaitMethod(int counter) {
            if (counter > 0) {
                counter--;
                Thread.onSpinWait();
            } else {
                Thread.yield();
            }
            return counter;
        }
    }

    public interface SequenceBarrier {
        long waitFor(long sequence) throws InterruptedException;
        long getCursor();
        boolean isAlerted();
        void alert();
        void clearAlert();
        void checkAlert() throws InterruptedException;
    }

    public static final class ProcessingSequenceBarrier implements SequenceBarrier {
        private final WaitStrategy waitStrategy;
        private final Sequence dependentSequence;
        private volatile boolean alerted = false;
        private final Sequence cursor;

        public ProcessingSequenceBarrier(WaitStrategy waitStrategy, Sequence dependentSequence, Sequence cursor) {
            this.waitStrategy = waitStrategy;
            this.dependentSequence = dependentSequence;
            this.cursor = cursor;
        }

        @Override
        public long waitFor(long sequence) throws InterruptedException {
            checkAlert();
            long availableSequence = waitStrategy.waitFor(sequence, cursor, dependentSequence, this);
            if (availableSequence < sequence) {
                return availableSequence;
            }
            return dependentSequence.get();
        }

        @Override
        public long getCursor() {
            return cursor.get();
        }

        @Override
        public boolean isAlerted() {
            return alerted;
        }

        @Override
        public void alert() {
            alerted = true;
            waitStrategy.signalAllWhenBlocking();
        }

        @Override
        public void clearAlert() {
            alerted = false;
        }

        @Override
        public void checkAlert() throws InterruptedException {
            if (alerted) {
                throw new InterruptedException("Thread alert requested on Disruptor barrier");
            }
        }
    }

    public static final class BatchEventProcessor<T> implements Runnable {
        private final SequenceSequenceBarrier sequenceBarrier;
        private final EventHandler<T> eventHandler;
        private final LifecycleAware lifecycleAware;
        private final Sequence sequence = new Sequence(-1L);
        private volatile boolean running = true;

        public BatchEventProcessor(DisruptorRingBuffer<T> ringBuffer, WaitStrategy waitStrategy, EventHandler<T> eventHandler) {
            this.sequenceBarrier = new SequenceSequenceBarrier(waitStrategy, ringBuffer.sequencer.cursor, ringBuffer.sequencer.gatingSequences);
            this.eventHandler = eventHandler;
            this.lifecycleAware = eventHandler instanceof LifecycleAware ? (LifecycleAware) eventHandler : null;
        }

        public Sequence getSequence() {
            return sequence;
        }

        @Override
        public void run() {
            if (lifecycleAware != null) {
                lifecycleAware.onStart();
            }

            try {
                long nextSequence = sequence.get() + 1L;
                while (running) {
                    try {
                        long availableSequence = sequenceBarrier.waitFor(nextSequence);
                        while (nextSequence <= availableSequence) {
                            // Event handling implementation placeholder
                            nextSequence++;
                        }
                        sequence.set(availableSequence);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        if (!running) {
                            break;
                        }
                    }
                }
            } finally {
                if (lifecycleAware != null) {
                    lifecycleAware.onShutdown();
                }
            }
        }

        public void halt() {
            running = false;
            sequenceBarrier.alert();
        }
    }

    public static final class SequenceSequenceBarrier {
        private final WaitStrategy waitStrategy;
        private final Sequence cursor;
        private final Sequence[] dependentSequences;
        private volatile boolean alerted = false;

        public SequenceSequenceBarrier(WaitStrategy waitStrategy, long cursorValue, Sequence[] dependentSequences) {
            this.waitStrategy = waitStrategy;
            this.cursor = new Sequence(cursorValue);
            this.dependentSequences = dependentSequences;
        }

        public long waitFor(long sequence) throws InterruptedException {
            checkAlert();
            return waitStrategy.waitFor(sequence, cursor, dependentSequences.length > 0 ? dependentSequences[0] : cursor, null);
        }

        public void alert() {
            alerted = true;
            waitStrategy.signalAllWhenBlocking();
        }

        public void checkAlert() throws InterruptedException {
            if (alerted) {
                throw new InterruptedException("Alerted");
            }
        }
    }

    public interface EventFactory<T> {
        T newInstance();
    }

    public interface EventHandler<T> {
        void onEvent(T event, long sequence, boolean endOfBatch) throws Exception;
    }

    public interface LifecycleAware {
        void onStart();
        void onShutdown();
    }

    public static final class EventProducer<T> {
        private final DisruptorRingBuffer<T> ringBuffer;

        public EventProducer(DisruptorRingBuffer<T> ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        public void onData(EventTranslator<T> translator) {
            long sequence = ringBuffer.next();
            try {
                T event = ringBuffer.get(sequence);
                translator.translateTo(event, sequence);
            } finally {
                ringBuffer.publish(sequence);
            }
        }
    }

    public interface EventTranslator<T> {
        void translateTo(T event, long sequence);
    }
}