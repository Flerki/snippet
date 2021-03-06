package com.amairovi.goetz_concurrency_in_practice.chapter_14;

import com.amairovi.goetz_concurrency_in_practice.GuardedBy;
import com.amairovi.goetz_concurrency_in_practice.ThreadSafe;

@ThreadSafe
public class ThreadGate {
    // CONDITION-PREDICATE: opened-since(n) (isOpen || generation > n)
    @GuardedBy("this")
    private boolean isOpen;
    @GuardedBy("this")
    private int generation;

    public synchronized void close() {
        isOpen = false;
    }

    public synchronized void open() {
        ++generation;
        isOpen = true;
        notifyAll();
    }

    // BLOCKS-UNTIL: opened-since (generation on entry)
    public synchronized void await() throws InterruptedException {
        int arrivalGeneration = generation;
        while (!isOpen && arrivalGeneration == generation) {
            wait();
        }
    }

}
