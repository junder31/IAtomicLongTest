package com.clearleap

/**
 * Created by johnunderwood on 8/26/15.
 */
class FileTransferAnalog implements Serializable {
    def final EdgeAnalog edge
    def final long id

    public FileTransferAnalog(EdgeAnalog edge, long id) {
        this.edge = edge
        this.id = id
    }


    @Override
    public String toString() {
        return "FileTransferAnalog{" +
                "edge=" + edge +
                ", id=" + id +
                '}';
    }
}
