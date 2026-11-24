package com.terminal.delivery;

import com.terminal.ingestion.TradeEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Fixed-capacity circular buffer holding the most recent trades, newest wins.
 *
 * <h2>Why a ring buffer and not a collection</h2>
 * <p>The requirement is "the last N, oldest evicted automatically". A
 * {@code LinkedList} would model that with {@code addLast}/{@code removeFirst}, but it
 * pays for it three ways:
 * <ul>
 *   <li><b>Allocation churn.</b> Every {@code add} allocates a {@code Node}, every
 *       eviction frees one. At 50 trades/s that is 50 short-lived objects per second
 *       feeding the young generation forever, for a buffer that logically only ever
 *       holds 50 references.</li>
 *   <li><b>Cache locality.</b> A linked list scatters its nodes across the heap, so
 *       walking it is pointer chasing — each hop is a probable L2/L3 miss. A circular
 *       array is one contiguous block: several references per cache line, and the
 *       prefetcher sees a sequential stride.</li>
 *   <li><b>Bookkeeping.</b> Array index arithmetic is a mask/modulo; a list has to
 *       maintain {@code next}/{@code prev} pointers on both neighbour nodes.</li>
 * </ul>
 * <p>A ring buffer also has a property that matters more than raw speed for a ticker:
 * <b>constant, predictable memory</b>. Its footprint is exactly {@code capacity}
 * references, forever, no matter whether the feed is silent or running at 10k/s. There
 * is no growth path that can surprise you at 3am during a volatility spike.
 *
 * <h2>Concurrency model: lock-free, single atomic claim per write</h2>
 * <p>Design: an {@link AtomicLong} sequence for the logical write position, plus an
 * {@link AtomicReferenceArray} of immutable {@link Slot} holders.
 * <ol>
 *   <li>The writer claims a position with {@code sequence.getAndIncrement()} — a single
 *       atomic RMW (one {@code LOCK XADD} on x86). Multiple Kafka listener threads may
 *       write concurrently; each gets a distinct, gap-free position.</li>
 *   <li>It builds a new immutable {@link Slot} and swaps it into
 *       {@code slots[position % capacity]} with one {@code AtomicReferenceArray.set}
 *       (a volatile store).</li>
 * </ol>
 * <p>The crucial trick is that <b>a slot is never mutated in place</b>. The slot object
 * is immutable and final-fielded, so a reader either sees the previous slot or the new
 * one — never a half-written trade with a fresh index. That is what makes the whole
 * structure safe without a lock: writers do not exclude readers, and readers do not
 * exclude writers.
 *
 * <p>Readers are wait-free: one volatile read of {@code sequence} gives a consistent
 * upper bound, then one volatile read per slot. If a writer laps the buffer mid-scan,
 * the sequence stamped on the slot no longer matches the position being read, and that
 * entry is skipped — a bounded, honest gap rather than a duplicated or torn trade.
 * (A gap is invisible in practice: with 50 slots and a real feed that is a ~1 s window;
 * a client would have to be unlucky by microseconds to notice one missing row.)
 *
 * <p>Strictly: writes are <b>lock-free</b> (two writers can never deadlock; under
 * contention one may have to retry the CAS inside {@code getAndIncrement}), and reads
 * are <b>wait-free</b>. Neither is a blocking algorithm, so no thread — least of all a
 * Kafka listener thread — can be parked by this buffer.
 */
@Component
public class TradeRingBuffer {

    /**
     * Immutable cell. `sequence` records which logical position this cell currently
     * holds, which is how a reader detects that it was overtaken mid-scan.
     */
    private static final class Slot {
        final long sequence;
        final TradeEvent trade;

        Slot(long sequence, TradeEvent trade) {
            this.sequence = sequence;
            this.trade = trade;
        }
    }

    private final int capacity;
    private final AtomicReferenceArray<Slot> slots;
    /** Monotonic count of writes ever made; also the next logical position. */
    private final AtomicLong sequence = new AtomicLong();

    public TradeRingBuffer(@Value("${terminal.delivery.snapshot-capacity:50}") int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive, was " + capacity);
        }
        this.capacity = capacity;
        this.slots = new AtomicReferenceArray<>(capacity);
    }

    /**
     * Appends a trade, overwriting the oldest entry when full. Never blocks.
     *
     * <p>{@code sequence} is always non-negative, so plain {@code %} is safe and
     * cheaper than {@code Math.floorMod}.
     */
    public void add(TradeEvent trade) {
        Objects.requireNonNull(trade, "trade must not be null");
        long position = sequence.getAndIncrement();
        slots.set((int) (position % capacity), new Slot(position, trade));
    }

    /**
     * Newest first, without mutating the buffer.
     *
     * <p>The returned list is a plain {@link ArrayList} owned by the caller — the
     * elements are immutable records, so handing it to a message converter (and from
     * there to an arbitrary number of sockets) is safe. This is why the snapshot needs
     * no lock and no copy-on-write structure: the buffer is never mutated in place
     * anyway, only re-pointed at new immutable cells.
     */
    public List<TradeEvent> snapshot() {
        long next = sequence.get();
        int available = (int) Math.min(next, capacity);
        List<TradeEvent> newestFirst = new ArrayList<>(available);
        for (int i = 0; i < available; i++) {
            long position = next - 1 - i;
            if (position < 0) {
                break;
            }
            Slot slot = slots.get((int) (position % capacity));
            // Discard anything the writer has already replaced while we scan.
            if (slot != null && slot.sequence == position) {
                newestFirst.add(slot.trade);
            }
        }
        return newestFirst;
    }

    /** Number of entries a snapshot would currently return (at most {@link #capacity()}). */
    public int size() {
        return (int) Math.min(sequence.get(), capacity);
    }

    public int capacity() {
        return capacity;
    }

    /** Total writes ever accepted — handy as a metric, monotonic. */
    public long appended() {
        return sequence.get();
    }
}
