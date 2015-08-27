package com.clearleap

/**
 * Created by johnunderwood on 8/26/15.
 */
class EdgeAnalog implements Serializable {
    def final String key
    def final int maxTransfers

    public EdgeAnalog(String key) {
        this.key = key
        maxTransfers = 1
    }


    @Override
    public String toString() {
        return "EdgeAnalog{" +
                "key='" + key + '\'' +
                '}';
    }
}
