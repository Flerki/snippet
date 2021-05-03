package com.amairovi;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class DefaultEntityLockerTest {

    @FunctionalInterface
    private interface Interruptible {
        void run() throws InterruptedException, ExecutionException;
    }

    @BeforeAll
    static void setup() {
        // TODO: cleanup this mess
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT %1$tL] [%4$-7s] %5$s %n");
        Level level = Level.INFO;
        Arrays.stream(Logger.getGlobal().getParent().getHandlers())
                .forEach(h -> h.setLevel(level));
        Logger.getGlobal().setLevel(level);
        Logger.getLogger("").setLevel(level);
    }

    @RepeatedTest(100)
    @Timeout(5)
    void whenThereAreSeveralThreadsForSameEntityThenAtMostOneIsAllowedToExecuteProtectedCode() {
        int amountOfThreads = 8;
        int amountOfOperations = 1000;
        List<Integer> idsToIncrement = new Random().ints(amountOfOperations, 0, amountOfThreads / 2)
                .boxed()
                .collect(toList());
        Map<Integer, Long> expectedResult = idsToIncrement.stream()
                .collect(groupingBy(identity(), counting()));

        DefaultEntityLocker<Integer> locker = new DefaultEntityLocker<>();
        ExecutorService executorService = Executors.newFixedThreadPool(amountOfThreads);
        Map<Integer, Long> actualResult = new ConcurrentHashMap<>();
        try {
            List<Callable<Integer>> collect = idsToIncrement.stream()
                    .map(idToIncrement -> (Callable<Integer>) () -> {
                        locker.lock(idToIncrement);
                        unsafeOperation(actualResult, idToIncrement);
                        locker.unlock(idToIncrement);
                        return idToIncrement;
                    })
                    .collect(toList());

            for (Future<Integer> future : executorService.invokeAll(collect)) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            fail(e);
        } finally {
            shutdownAndAwaitTermination(executorService);
        }

        assertThat(actualResult).isEqualTo(expectedResult);
    }

    @Test
    @Timeout(5)
    void whenThereAreDifferentEntitiesThenTheyCanBeLockedConcurrently() {
        DefaultEntityLocker<Integer> locker = new DefaultEntityLocker<>();
        int amountOfThreads = 8;

        CountDownLatch latch = new CountDownLatch(amountOfThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(amountOfThreads);
        try {

            // amount of entities == amount of threads
            // so each thread should count down once to proceed
            IntStream.range(0, amountOfThreads)
                    .mapToObj(id -> (Runnable) () -> {
                        locker.lock(id);
                        latch.countDown();
                        runInterruptibleSafely(latch::await);

                        locker.unlock(id);
                    })
                    .forEach(executorService::submit);
        } finally {
            shutdownAndAwaitTermination(executorService);
        }
        runInterruptibleSafely(
                () -> assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue()
        );
    }

    @Test
    @Timeout(5)
    void whenReentrantLockTakesPlaceThenItShouldSucceed() {
        DefaultEntityLocker<Integer> locker = new DefaultEntityLocker<>();
        AtomicInteger counter = new AtomicInteger(0);

        CountDownLatch latch = new CountDownLatch(1);
        Integer id = 1;
        locker.lock(id);

        locker.lock(id);
        counter.incrementAndGet();
        locker.unlock(id);
        assertThat(counter.get()).isEqualTo(1);

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            locker.lock(id);
            latch.countDown();
            counter.incrementAndGet();
            locker.unlock(id);
        });

        counter.incrementAndGet();
        runInterruptibleSafely(() -> assertThat(latch.await(1, TimeUnit.SECONDS)).isFalse());
        assertThat(counter.get()).isEqualTo(2);
        locker.unlock(id);

        runInterruptibleSafely(future::get);

        assertThat(counter.get()).isEqualTo(3);
    }

    private void runInterruptibleSafely(final Interruptible interruptible) {
        try {
            interruptible.run();
        } catch (InterruptedException e) {
            fail("Should not be interrupted", e);
        } catch (ExecutionException e) {
            fail("Should be executed successfully", e);
        }
    }

    private void unsafeOperation(final Map<Integer, Long> actualResult, final Integer idToIncrement) {
        Long val = actualResult.getOrDefault(idToIncrement, 0L);
        actualResult.put(idToIncrement, val + 1);
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}