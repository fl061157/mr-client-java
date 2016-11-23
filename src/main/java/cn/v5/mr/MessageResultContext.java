package cn.v5.mr;

public class MessageResultContext {
    private int result;
    private long messageId;
    private byte[] bytes;

    private MessageCallback pubCallback;

    public MessageResultContext() {
        result = -1;
    }

    public MessageResultContext(int ret, long mid, MessageCallback pcb, byte[] bytes) {
        this.result = ret;
        this.pubCallback = pcb;
        this.bytes = bytes;
        this.messageId = mid;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public MessageCallback getPubCallback() {
        return pubCallback;
    }

    public void setPubCallback(MessageCallback pubCallback) {
        this.pubCallback = pubCallback;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MessageResultContext: result=").append(result);
        sb.append(",messageId=").append(messageId);
        sb.append(",bytes=").append(bytes);
        return sb.toString();
    }
}
