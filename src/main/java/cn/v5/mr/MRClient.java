package cn.v5.mr;

public interface MRClient {

    void start();

    void stop();

    void stopAndWait() throws InterruptedException;

    void stopAndWait(int ms) throws InterruptedException;

    boolean isRunning();

    int pub(String topic, byte[] bytes);

    public int pub(String topic, byte[] bytes, int priority);

    int pubDelay(String topic, byte[] bytes, int delay_secs);

    public int pubDelay(String topic, byte[] bytes, int delay_secs, int priority);

    int pubSort(String topic, byte[] bytes, String sortKey);

    public int pubSort(String topic, byte[] bytes, String sortKey, int priority);

    int pubRepeat(String topic, byte[] bytes, int interval, int times);

    public int pubRepeat(String topic, byte[] bytes, int interval, int times, int priority);

    int pubDirect(String topic, byte[] bytes, MessageResultContext mrc);

    public int pubDirect(String topic, byte[] bytes, MessageResultContext mrc, int priority);

    boolean pub(String topic, byte[] bytes, MessageCallback pcb);

    public boolean pub(String topic, byte[] bytes, MessageCallback pcb, int priority);

    boolean pubDelay(String topic, byte[] bytes, int delay_secs, MessageCallback pcb);

    public boolean pubDelay(String topic, byte[] bytes, int delay_secs, MessageCallback pcb, int priority);

    boolean pubSort(String topic, byte[] bytes, String sortKey, MessageCallback pcb);

    public boolean pubSort(String topic, byte[] bytes, String sortKey, MessageCallback pcb, int priority);

    boolean pubRepeat(String topic, byte[] bytes, int interval, int times, MessageCallback pcb);

    public boolean pubRepeat(String topic, byte[] bytes, int interval, int times, MessageCallback pcb, int priority);

    boolean pubDirect(String topic, byte[] bytes, MessageCallback pcb);

    public boolean pubDirect(String topic, byte[] bytes, MessageCallback pcb, int priority);

    boolean pub(String topic);

    boolean sub(String topic, MessageCallback pcb);

    boolean sub(String topic, int perfetchSize, MessageCallback pcb);

    boolean sub(String topic, int perfetchSize, int timeout, MessageCallback pcb);

    boolean ack(String topic, long mid);

    boolean ack(String topic, long mid, int status);

    boolean ack(String topic, long mid, int status, byte[] bytes);

    boolean unsub(String topic);

    boolean cancel(String topic, long mid);

    boolean pubUnsafe(String topic, byte[] bytes);

    public boolean pubUnsafe(String topic, byte[] bytes, int priority);

    boolean subUnsafe(String topic, MessageCallback pcb);

    boolean subUnsafe(String topic, int perfetchSize, MessageCallback pcb);
}
