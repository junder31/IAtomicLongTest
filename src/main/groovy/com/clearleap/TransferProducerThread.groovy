package com.clearleap

import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceNotActiveException
import groovy.util.logging.Slf4j

/**
 * Created by johnunderwood on 8/26/15.
 */
@Slf4j
class TransferProducerThread extends Thread {
    public static final long SLEEP_TIME_MS = 50l
    def volatile boolean stop = false

    def final HazelcastInstance hazelcastInstance
    def final Queue<FileTransferAnalog> fileTransferQueue
    def final List<EdgeAnalog> edges
    def long transferNum = 0;
    def final Random rand = new Random()

    public TransferProducerThread(List<EdgeAnalog> edges) {
        hazelcastInstance = Hazelcast.newHazelcastInstance()
        fileTransferQueue = hazelcastInstance.getQueue("fileTransferQueue")
        this.edges = edges
    }

    @Override
    public void run() {

        while(!stop) {
            try {
                while(!stop && fileTransferQueue.size() < 1024) {
                    def edge = edges.get(rand.nextInt(edges.size()))
                    def fileTransfer = new FileTransferAnalog(edge, transferNum++)
                    fileTransferQueue.offer(fileTransfer)
                    log.debug("File Transfer submitted: ${fileTransfer}")
                }
                sleep(SLEEP_TIME_MS)
            } catch (HazelcastInstanceNotActiveException ex) {
                log.info("Aborting because hazelcast shutdown.")
                break;
            } catch (Exception ex) {
                log.error("Error submitting transfer", ex)
            }
        }

        hazelcastInstance.shutdown()
    }
}
