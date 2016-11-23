package cn.v5.mr;

public interface MessageCallback {
    void on(int status, long mid, byte[] bytes);
}
