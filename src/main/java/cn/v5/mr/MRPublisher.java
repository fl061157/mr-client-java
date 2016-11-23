package cn.v5.mr;

import java.util.concurrent.Future;

public interface MRPublisher {
    boolean syncPub(String topic, byte[] data);

    boolean syncPubDelay(String topic, byte[] data, int sec);

    boolean syncPubSort(String topic, byte[] data, String sortKey);

    MessageResultContext syncPubDirect(String topic, byte[] data);

    Future<MessageResultContext> asyncPub(String topic, byte[] data);

    boolean asyncPub(String topic, byte[] data, MessageCallback callback);

    boolean asyncPubDelay(String topic, byte[] data, int sec, MessageCallback callback);

    boolean asyncPubSort(String topic, byte[] data, String sortKey, MessageCallback callback);

    boolean asyncPubDirect(String topic, byte[] data, MessageCallback callback);

    boolean unsafePub(String topic, byte[] data);

    void setPriority(int priority);

}
