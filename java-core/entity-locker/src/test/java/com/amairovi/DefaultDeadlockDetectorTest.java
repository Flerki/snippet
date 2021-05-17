package com.amairovi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class DefaultDeadlockDetectorTest {

    private final DefaultDeadlockDetector<Number> underTest = new DefaultDeadlockDetector<>();

    @Test
    void whenThereIsNoDeadlockThenDoNothing() {
        Thread thread1 = new Thread();
        Integer id = 1;
        Thread thread2 = new Thread();

        LockingData<Number> data = new LockingData<>();
        data.addLockOwning(id, thread1);

        underTest.check(id, thread2, data);
    }

    @Test
    void whenThereIsDeadlockThenShouldThrowException() {
        Thread thread1 = new Thread();
        Integer id1 = 1;
        Thread thread2 = new Thread();
        Integer id2 = 2;

        LockingData<Number> data = new LockingData<>();
        data.addLockOwning(id1, thread1);
        data.addLockOwning(id2, thread2);

        data.addLockAcquiring(id1, thread2);
        data.addLockAcquiring(id2, thread1);

        assertThatThrownBy(() -> underTest.check(id2, thread1, data))
                .isInstanceOf(DeadlockDetectedException.class);
    }

    @Test
    void whenThereIsDeadlockInvolvingGlobalLockThenShouldThrowException() {
        Thread thread1 = new Thread();
        Integer id1 = 1;
        Thread thread2 = new Thread();
        Integer id2 = 2;

        LockingData<Number> data = new LockingData<>();
        data.addLockOwning(id1, thread1);
        data.addLockOwning(id2, thread2);
        data.addGlobalLockAcquiring(thread1);

        assertThatThrownBy(() -> underTest.check(id1, thread2,data))
                .isInstanceOf(DeadlockDetectedException.class);
    }

    @Test
    void whenThereIsAGlobalLockAndOtherThreadDoesNotHaveAnyEntitiesAcquireThenShouldNotThrowException() {
        Thread thread1 = new Thread();
        Thread thread2 = new Thread();

        Integer id1 = 1;
        LockingData<Number> data = new LockingData<>();
        data.addLockOwning(id1, thread1);
        data.addGlobalLockAcquiring(thread1);

        assertDoesNotThrow(() -> underTest.check(id1, thread2, data));
    }

    @Test
    void whenThereIsDeadlockAndSeveralLockAcquiredByBothThreadsThenShouldThrowException() {
        Thread thread1 = new Thread();
        Integer id1 = 1;
        Integer id3 = 3;
        Thread thread2 = new Thread();
        Integer id2 = 2;
        Integer id4 = 4;

        LockingData<Number> data = new LockingData<>();
        data.addLockOwning(id1, thread1);
        data.addLockOwning(id3, thread1);
        data.addLockOwning(id2, thread2);
        data.addLockOwning(id4, thread2);

        data.addLockAcquiring(id3, thread2);
        data.addLockAcquiring(id2, thread1);

        assertThatThrownBy(() -> underTest.check(id2, thread1, data))
                .isInstanceOf(DeadlockDetectedException.class);
    }

    @Test
    void whenThereIsChainedDeadlockThenShouldThrowException() {
        int amountOfThreads = 16;
        List<Thread> threads = new ArrayList<>(amountOfThreads);

        for (int i = 0; i < amountOfThreads; i++) {
            threads.add(new Thread());
        }

        LockingData<Number> data = new LockingData<>();
        for (int id = 0; id < amountOfThreads; id++) {
            data.addLockOwning(id, threads.get(id));
            Integer nextId = (id + 1) % amountOfThreads;
            data.addLockAcquiring(nextId, threads.get(id));
        }

        for (int i = 0; i < amountOfThreads; i++) {
            int id = i;
            Integer nextId = (id + 1) % amountOfThreads;
            assertThatThrownBy(() -> underTest.check(nextId, threads.get(id), data))
                    .isInstanceOf(DeadlockDetectedException.class);
        }
    }

}