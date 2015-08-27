package com.clearleap

import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceNotActiveException
import com.hazelcast.core.IAtomicLong
import groovy.util.logging.Slf4j

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

/**
 * Created by johnunderwood on 8/26/15.
 */
@Slf4j
class DistributionJobThread extends Thread {
    public static final Map<String, AtomicLong> realTransferCounts = new ConcurrentHashMap<>()
    public static final long TRANSFER_TIME_MIN = 100l
    public static final long TRANSFER_TIME_MAX = 1000l
    def volatile boolean stop = false

    private final ExecutorService transferExecutor = Executors.newCachedThreadPool()
    def final HazelcastInstance hazelcastInstance
    def final Queue<FileTransferAnalog> fileTransferQueue
    def final Random rand = new Random()
    def final int num

    public DistributionJobThread(int num) {
        this.num = num
        hazelcastInstance = Hazelcast.newHazelcastInstance()
        fileTransferQueue = hazelcastInstance.getQueue("fileTransferQueue")
    }

    @Override
    public void run() {
        log.info("New DistributionJobThread started.")
        for (int i = 0; i < num; i++) {
            transferExecutor.submit(new FileTransferRunnable())
        }

        while (!stop) {
            sleep(100)
        }

        hazelcastInstance.shutdown()
        transferExecutor.shutdown()
        log.info("DistributionJobThread stopped.")
    }

    public static AtomicLong getRealTransferCount(EdgeAnalog edge) {
        synchronized (realTransferCounts) {
            def count = realTransferCounts.get(edge.key)
            if (!count) {
                count = new AtomicLong(0)
                realTransferCounts.put(edge.key, count)
            }

            return count
        }
    }

    public static boolean compareAndIncrement(EdgeAnalog edge, IAtomicLong hazelcastCount,
                                              long expectedValue, AtomicLong realCount) {
        synchronized (realTransferCounts) {
            long newValue = expectedValue + 1
            if (hazelcastCount.compareAndSet(expectedValue, newValue)) {
                def realValue = realCount.incrementAndGet()
                if (realValue != newValue) {
                    log.error("Detected bad transfer count ${edge} RealCount: ${realValue}, " +
                            "HazlecastCount: ${newValue}, HazelcastPreviousValue: ${expectedValue}")
                }
                return true
            } else {
                return false
            }
        }
    }

    public static long decrementAndGet(EdgeAnalog edge, IAtomicLong hazelcastCount, AtomicLong realCount) {
        synchronized (realTransferCounts) {
            def hazelcastValue =  hazelcastCount.decrementAndGet()
            def realValue = realCount.decrementAndGet()
            if (realValue != hazelcastValue) {
                log.error("Detected bad transfer count ${edge} RealCount: ${realValue}, " +
                        "HazlecastCount: ${hazelcastValue}")
            }
            return hazelcastValue
        }
    }

    public class FileTransferRunnable implements Runnable {
        @Override
        void run() {
            AtomicLong realCount
            boolean claimed = false
            try {
                while (!stop) {
                    def fileTransfer = fileTransferQueue.poll()
                    if (fileTransfer) {
                        claimed = false
                        IAtomicLong transferCount = hazelcastInstance.getAtomicLong(fileTransfer.edge.key)
                        realCount = getRealTransferCount(fileTransfer.edge)
                        long currentCount = transferCount.get()
                        while (!stop && currentCount < fileTransfer.edge.maxTransfers) {
                            if (compareAndIncrement(fileTransfer.edge, transferCount, currentCount, realCount)) {
                                log.debug("Claimed transfer ${fileTransfer}. EdgeCount = ${currentCount + 1}")
                                claimed = true
                                def interval = (int) (TRANSFER_TIME_MAX - TRANSFER_TIME_MIN)
                                def sleepTime = rand.nextInt(interval) + TRANSFER_TIME_MIN
                                sleep(sleepTime)
                                long newCount = decrementAndGet(fileTransfer.edge, transferCount, realCount)
                                log.debug("Finished transfer ${fileTransfer}. EdgeCount = ${newCount}")
                                break
                            }

                            currentCount = transferCount.get()
                        }

                        if (!claimed) {
                            fileTransferQueue.offer(fileTransfer)
                        }
                    }
                }
            } catch (HazelcastInstanceNotActiveException ex) {
                log.info("Aborting because hazelcast shutdown.")
            } catch (Exception ex) {
                log.error("FileTransfer error", ex)
            } finally {
                if(claimed && realCount) {
                    realCount.decrementAndGet()
                }
            }
        }
    }
}
