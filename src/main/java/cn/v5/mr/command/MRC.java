package cn.v5.mr.command;

import cn.v5.mr.MRClient;
import cn.v5.mr.MRConnectionManager;
import cn.v5.mr.impl.MRConnectionManagerImpl;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MRC {

    private static final Logger logger = LoggerFactory.getLogger(MRC.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "host", true, "mr server host url");
        options.addOption("s", "sub", true, "sub topic");
        options.addOption("p", "pub", true, "pub topic");
        options.addOption("d", "display", false, "display content");
        options.addOption("c", "content", true, "pub content");
        options.addOption("n", "num", true, "pub count");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String url = StringUtils.trimToNull(cmd.getOptionValue("h"));
        String subTopic = StringUtils.trimToNull(cmd.getOptionValue("s"));
        String pubTopic = StringUtils.trimToNull(cmd.getOptionValue("p"));
        String content = StringUtils.trimToNull(cmd.getOptionValue("c"));
        boolean display = cmd.hasOption("d");
        int count = NumberUtils.toInt(cmd.getOptionValue("n"), 0);

        if (url != null && subTopic != null) {
            sub(url, subTopic, display);
        } else if (url != null && pubTopic != null && content != null && count > 0) {
            pub(url, pubTopic, count, content, 0);
        } else {
            usage(options);
        }
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("mrc", options);
    }

    public static void pub(String url, String topic, final int count, String content, final int type) throws UnsupportedEncodingException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(128);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        byte[] data = content.getBytes("UTF-8");
        if (type == 1) {
            long stime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                int ret = c.pub(topic, data);
                if (ret != 0) {
                    logger.error("pub error.");
                    break;
                }
            }
            long use = System.currentTimeMillis() - stime;
            double sp = (double) count / (double) use * 1000.0;
            System.out.println("sync pub speed " + String.format("%,.0f", sp) + "/s");
        } else {
            AtomicLong send_cb_count = new AtomicLong(0);
            AtomicLong error_count = new AtomicLong(0);
            AtomicBoolean running = new AtomicBoolean(true);
            long send_count = 0;
            final long stimes[] = {System.currentTimeMillis(), 0};
            final long[] preCount = {0, 0};
            final long[] preTime = {System.currentTimeMillis()};
            new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        break;
                    }
                    long now = System.currentTimeMillis();
                    long nowCount = send_cb_count.get();
                    long nowError = error_count.get();
                    long use = now - preTime[0];
                    long n = nowCount - preCount[0];
                    long errn = nowError - preCount[1];
                    double sp = (double) n / (double) use * 1000.0;
                    preTime[0] = now;
                    preCount[0] = nowCount;
                    preCount[1] = nowError;
                    if (errn == 0) {
                        System.out.println(String.format("async pub speed %,.0f/s, send count %,d", sp, nowCount));
                    } else {
                        System.out.println(String.format("async pub speed %,.0f/s, send count %,d, error count %,d", sp, nowCount, errn));
                    }
                    if (nowCount >= count) {
                        running.set(false);
                        break;
                    }
                }
            }).start();

            while (send_count++ < count) {
                boolean ret = c.pub(topic, data, (status, id, bytes) -> {
                    send_cb_count.incrementAndGet();
                    if (status != 0) {
                        error_count.incrementAndGet();
                        //logger.error("async pub error:{}", status);
                    }
                });
                while (send_count - send_cb_count.get() > 980) {
                    Thread.sleep(1);
                }
                if (!ret) {
                    send_count--;
                }
            }

            while (running.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        mrConnectionManager.shutdown();
        executorService.shutdown();
    }

    public static void sub(String url, String topic, boolean display) {
        ExecutorService executorService = Executors.newFixedThreadPool(128);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        AtomicLong count = new AtomicLong(0);
        c.sub(topic, (status, id, bytes) -> {
            count.incrementAndGet();
            if (status != 0) {
                logger.error("cub error : {}", status);
            }
            if (display) {
                String content = StringUtils.toEncodedString(bytes, Charset.forName("UTF-8"));
                System.out.println(String.format("sub topic:%s, content:%s", topic, content));
            }
            c.ack(topic, id);
        });


        final long[] preCount = {0};
        final long[] preTime = {System.currentTimeMillis()};
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
                long now = System.currentTimeMillis();
                long nowCount = count.get();
                long use = now - preTime[0];
                long n = nowCount - preCount[0];
                double sp = (double) n / (double) use * 1000.0;
                preTime[0] = now;
                preCount[0] = nowCount;
                System.out.println(String.format("sub speed %,.0f/s all count %,d", sp, nowCount));
            }
        }).start();
    }
}
