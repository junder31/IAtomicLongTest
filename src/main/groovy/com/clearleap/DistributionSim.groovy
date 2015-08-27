package com.clearleap

import groovy.util.logging.Slf4j

/**
 * Created by johnunderwood on 8/24/15.
 */
@Slf4j
class DistributionSim {
    public static List<String> loadEdgeKeys() {
        List<String> edgeKeys = []
        def reader = new InputStreamReader(DistributionSim.classLoader.getResourceAsStream("edge_keys.txt"))
        try {
            String key
            while ((key = reader.readLine()) != null) {
                edgeKeys.add(key)
            }
        } finally {
            reader.close()
        }

        return edgeKeys
    }

    public static void main(String[] args) {
        List<EdgeAnalog> edges = loadEdgeKeys().collect { new EdgeAnalog(it) }
        TransferProducerThread transferProducerThread = new TransferProducerThread(edges)
        transferProducerThread.start()

        List<DistributionJobThread> djThreads = []
        def rand = new Random()

        for(int i = 0; i < 4; i++) {
            def distributionJobThread = new DistributionJobThread(10)
            djThreads.add(distributionJobThread)
            distributionJobThread.start()
            log.info("Starting ${distributionJobThread}")
        }

        sleep(30000)

        def i = rand.nextInt(djThreads.size())
        def distributionJobThread = djThreads.remove(i)
        log.info("Shutting down ${distributionJobThread}")
        distributionJobThread.stop = true

        sleep(15000)

        djThreads.each {
            it.stop = true
        }
        transferProducerThread.stop = true
    }
}
