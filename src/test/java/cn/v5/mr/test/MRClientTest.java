package cn.v5.mr.test;

import cn.v5.mr.MRClient;
import cn.v5.mr.MRConnectionManager;
import cn.v5.mr.MessageResultContext;
import cn.v5.mr.impl.MRConnectionManagerImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MRClientTest {
    private static Logger logger = LoggerFactory.getLogger(MRClientTest.class);

    static String strs = "01234567890abcdefghijklmnopqrstuvwxyz";
    static int strs_len = strs.length();
    static Random random = new Random();

    public static String getRandomString(final int len) {
        StringBuilder sb = new StringBuilder(len);
        sb.append("h");
        int l = random.nextInt(len < 1 ? 1 : len);
        l = l < 1 ? len : l;
        for (int i = 0; i < l; i++) {
            int r = random.nextInt(strs_len);
            sb.append(strs.charAt(r));
        }
        return sb.toString();
    }

    public static void base_test(final String url, boolean pub, boolean sub) throws Exception {
        logger.info("main start");
        String topic = "testTopic23231";

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        if (sub) {
            c.sub(topic, (status, id, bytes) -> {
                try {
                    logger.debug("recv message id {}, len {}, content:'{}'", id, bytes.length, new String(bytes, "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                c.ack(topic, id);
            });
        }

        if (pub) {
            int n = 10;
            while (n-- > 0) {
                int err = c.pub(topic, ("_" + n).getBytes("UTF-8"));
                if (err != 0) {
                    logger.debug("sync pub err {}", err);
                }
                c.pub(topic, ("_" + n).getBytes("UTF-8"), (status, id, bytes) -> {
                    logger.debug("async pub cb status " + status);
                });
            }
        }

        do {
            Thread.sleep(2000);
        } while (true);

        //c.stop();
        //logger.info("main stop");
    }

    public static void sub_speed_test(final String url, final String topic) throws Exception {
        logger.info("main start");
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        AtomicLong sub_count = new AtomicLong(0);
        final AtomicLong[] stimes = {new AtomicLong(System.currentTimeMillis()), new AtomicLong(0)};
        c.sub(topic, (status, id, bytes) -> {
            //logger.info("xxxx {}", id);
            c.ack(topic, id);
            long sc = sub_count.incrementAndGet();

            if (sc % 40000 == 0) {
                long now = System.currentTimeMillis();
                long pre_time = stimes[0].getAndSet(now);
                long pre_count = stimes[1].getAndSet(sc);
                double s = (double) (sc - pre_count) * 1000.0 / (double) (now - pre_time);
                logger.debug("async sub all count {}, this time count {},  speed {}", sc, (sc - pre_count), (long) s);
            }
        });

        do {
            Thread.sleep(2000);
        } while (true);
        //c.stop();
        //logger.info("main stop");
    }

    static class PubTask implements Runnable {

        private final AtomicLong count;
        private final MRClient c;
        private final String topic;
        private int num;

        public PubTask(MRClient c, String topic, int num, AtomicLong count) {
            this.c = c;
            this.topic = topic;
            this.num = num;
            this.count = count;
        }

        @Override
        public void run() {
            int err = 0;
            while (num-- > 0) {
                try {
                    err = c.pub(topic, ("ok, sync message " + num + ".").getBytes("UTF-8"));
                    if (err != 0) {
                        logger.debug("sync pub err {}", err);
                        num++;
                        Thread.sleep(4);
                        continue;
                    }
                    count.incrementAndGet();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void pub_speed_test(final String url, final String topic) throws Exception {
        logger.info("main start");
        ExecutorService executorService = Executors.newFixedThreadPool(80);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        final int all_count = 400000;
        final AtomicLong s_count = new AtomicLong(0);
        final long sts[] = {System.currentTimeMillis(), 0};

        PubTask pts[] = new PubTask[1];
        for (int i = 0; i < pts.length; i++) {
            pts[i] = new PubTask(c, topic, 400000, s_count);
            executorService.submit(pts[i]);
        }

        long stime = System.currentTimeMillis();
        long pre_count = 0;
        while (true) {
            long cc = s_count.get();
            if (cc > 0 && cc % 40000 < 10) {
                long now = System.currentTimeMillis();
                double s = (double) (cc - pre_count) * 1000.0 / (double) (now - stime);
                stime = now;
                logger.debug("sync pub all count {}, this time count {},  speed {}", cc, (cc - pre_count), (long) s);
                pre_count = cc;
            }
            if (cc >= all_count) {
                break;
            }
            Thread.sleep(1);
        }

        AtomicLong send_cb_count = new AtomicLong(0);
        long send_count = 0;
        final long stimes[] = {System.currentTimeMillis(), 0};
        while (send_count++ < all_count) {
            boolean ret = c.pub(topic, ("ok, async message " + send_count + ".").getBytes("UTF-8"), (status, id, bytes) -> {
                if (status != 0) {
                    return;
                }
                long cbcount = send_cb_count.incrementAndGet();

                if (cbcount % 40000 == 0) {
                    long now = System.currentTimeMillis();
                    double s = (double) (cbcount - stimes[1]) * 1000.0 / (double) (now - stimes[0]);
                    stimes[0] = now;
                    stimes[1] = cbcount;
                    logger.debug("async pub count {}, speed {}", cbcount, (long) s);
                }
            });
            while (send_count - send_cb_count.get() > 650) {
                Thread.sleep(2);
            }
            if (!ret) {
                send_count--;
            }
        }

        do {
            Thread.sleep(2000);
        } while (true);
        //c.stop();
        //logger.info("main stop");
    }


    public static void quit_test(final String url) throws Exception {
        logger.info("quit test start");
        String topic = "quit_test";

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        MRConnectionManager mrConnectionManager2 = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        mrConnectionManager2.init(url, executorService);
        MRClient subc = mrConnectionManager.getMRClient();
        MRClient pubc = mrConnectionManager2.getMRClient();

        subc.sub(topic, 20, 60, (status, id, bytes) -> {
            try {
                Thread.sleep(12000);
                String content = StringUtils.trimToNull(new String(bytes, "UTF-8"));
                logger.debug("recv message id {}, len {}, content:'{}'", id, bytes.length, content);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            subc.ack(topic, id);
        });

        int n = 10;
        while (n-- > 0) {
            int err = pubc.pub(topic, ("_" + n).getBytes("UTF-8"));
            if (err != 0) {
                logger.debug("sync pub err {}", err);
            }
        }

        Thread.sleep(2000);

        logger.debug("unsub topic {}", topic);
        subc.unsub(topic);
        logger.debug("c.stop");
        try {
            mrConnectionManager.shutdownAndWait(10);
        }catch (InterruptedException e){

        }
        mrConnectionManager2.shutdownAndWait();
        executorService.shutdown();
        logger.info("main stop");

    }

    public static void delay_test(final String url) throws Exception {
        logger.info("delay test start");
        String topic = "delay_test";

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        c.sub(topic, (status, id, bytes) -> {
            try {
                String content = StringUtils.trimToNull(new String(bytes, "UTF-8"));
                logger.debug("recv message id {}, len {}, content:'{}'", id, bytes.length, content);
                if (content != null) {
                    String[] ss = StringUtils.split(content, ',');
                    if (ss.length == 2) {
                        long time = NumberUtils.toLong(ss[0]);
                        long sep = NumberUtils.toLong(ss[1]);
                        long now = System.currentTimeMillis();
                        double dx = (now - time) / 1000.0;
                        logger.debug("pub time {}, now {}, sep {}, dx {}", time, now, sep, dx);
                    }
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            c.ack(topic, id);
        });

        int n = 10;
        while (n-- > 0) {
            int err = c.pubDelay(topic, (System.currentTimeMillis() + "," + n).getBytes("UTF-8"), n);
            if (err != 0) {
                logger.debug("sync pub err {}", err);
            }
            final int cn = n;
            c.pubDelay(topic, (System.currentTimeMillis() + "," + n).getBytes("UTF-8"), n, (status, id, bytes) -> {
                logger.debug("async pub cb status {}, mid {}", status, id);
                if (cn == 8) {
                    logger.debug("cancel message id {}", id);
                    c.cancel(topic, id);
                }
            });
        }

        do {
            Thread.sleep(2000);
        } while (true);

        //c.stop();
        //logger.info("main stop");
    }

    public static void sort_test(final String url) throws Exception {
        logger.info("sort test start");
        String topic = "sort_test";

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        c.sub(topic, (status, id, bytes) -> {
            try {
                String content = StringUtils.trimToNull(new String(bytes, "UTF-8"));
                logger.debug("recv message id {}, len {}, content:'{}'", id, bytes.length, content);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            c.ack(topic, id);
        });

        int n = 10;
        while (n-- > 0) {
            int err = c.pubSort(topic, (System.currentTimeMillis() + "," + n).getBytes("UTF-8"), "sort");
            if (err != 0) {
                logger.debug("sync pub err {}", err);
            }
            c.pubSort(topic, (System.currentTimeMillis() + "," + n).getBytes("UTF-8"), "sort", (status, id, bytes) -> {
                logger.debug("async pub cb status {}, mid {}", status, id);
            });
        }

        do {
            Thread.sleep(2000);
        } while (true);

        //c.stop();
        //logger.info("main stop");
    }

    public static void repeat_test(final String url) throws Exception {
        logger.info("repeat test start");
        String topic = "repeat_test";

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        c.sub(topic, (status, id, bytes) -> {
            try {
                String content = StringUtils.trimToNull(new String(bytes, "UTF-8"));
                logger.debug("recv message id {}, len {}, content:'{}'", id, bytes.length, content);
                if (content != null) {
                    String[] ss = StringUtils.split(content, ',');
                    if (ss.length == 3) {
                        long time = NumberUtils.toLong(ss[0]);
                        long sep = NumberUtils.toLong(ss[1]);
                        long now = System.currentTimeMillis();
                        double dx = (now - time) / 1000.0;
                        logger.debug("mid {}, pub time {}, now {}, sep {}, dx {}", id, time, now, sep, dx);
                    }
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            c.ack(topic, id);
        });

        int err = c.pubRepeat(topic, (System.currentTimeMillis() + "," + "5,10").getBytes("UTF-8"), 5, 10);
        if (err != 0) {
            logger.debug("sync pub err {}", err);
        }
        c.pubRepeat(topic, (System.currentTimeMillis() + "," + "2,10").getBytes("UTF-8"), 2, 10, (status, id, bytes) -> {
            logger.debug("async pub cb status {}, mid {}", status, id);
        });

        do {
            Thread.sleep(2000);
        } while (true);

        //c.stop();
        //logger.info("main stop");
    }

    public static void direct_test(final String url) throws Exception {
        logger.info("direct test start");
        String topic = "direct_test";

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        c.sub(topic, (status, id, bytes) -> {
            try {
                String content = StringUtils.trimToNull(new String(bytes, "UTF-8"));
                logger.debug("recv message id {}, len {}, content:'{}'", id, bytes.length, content);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            c.ack(topic, id, 0, bytes);
        });

        int n = 10;
        while (n-- > 0) {
            MessageResultContext messageResultContext = new MessageResultContext();
            int err = c.pubDirect(topic, (System.currentTimeMillis() + "").getBytes("UTF-8"), messageResultContext);
            if (err != 0) {
                logger.debug("sync pub err {}", err);
            } else {
                String content = messageResultContext.getBytes() == null ?
                        null : StringUtils.trimToNull(new String(messageResultContext.getBytes(), "UTF-8"));
                logger.debug("result {}, mid {}, return content {}", messageResultContext.getResult(),
                        messageResultContext.getMessageId(), content);
                if (content != null) {
                    long time = NumberUtils.toLong(content);
                    long now = System.currentTimeMillis();
                    double dx = (now - time) / 1000.0;
                    logger.debug("sync direct result mid {}, pub time {}, now {}, dx {}",
                            messageResultContext.getMessageId(), time, now, dx);
                }
            }
            c.pubDirect(topic, (System.currentTimeMillis() + "").getBytes("UTF-8"), (status, id, bytes) -> {
                String content = null;
                try {
                    content = bytes == null ? null : StringUtils.trimToNull(new String(bytes, "UTF-8"));
                    logger.debug("result {}, mid {}, return content {}", status, id, content);
                    if (content != null) {
                        long time = NumberUtils.toLong(content);
                        long now = System.currentTimeMillis();
                        double dx = (now - time) / 1000.0;
                        logger.debug("async direct result mid {}, pub time {}, now {}, dx {}",
                                messageResultContext.getMessageId(), time, now, dx);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            });
        }
        do {
            Thread.sleep(2000);
        } while (true);

        //c.stop();
        //logger.info("main stop");
    }

    public void testBase() {
        final String url = "tcp://127.0.0.1:9000";

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        executorService.submit(() -> {
            try {
                base_test(url, false, true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.submit(() -> {
            try {
                base_test(url, true, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static void recv_only(String url, String topic) throws Exception {
        logger.info("recv only test start");

        ExecutorService executorService = Executors.newFixedThreadPool(8);
        MRConnectionManager mrConnectionManager = new MRConnectionManagerImpl();
        mrConnectionManager.init(url, executorService);
        MRClient c = mrConnectionManager.getMRClient();

        c.sub(topic, (status, id, bytes) -> {
                logger.debug("recv message id {}, len {}", id, bytes.length);
        });

    }

    public static void main(String[] args) throws Exception {
        final String url = "tcp://127.0.0.1:18001";

        quit_test(url);
        logger.info("quit test end.");
        //delay_test(url);
        //sort_test(url);
        //repeat_test(url);
        //direct_test(url);

        //recv_only("tcp://192.168.1.151:8010", "TCPSERVER_LOGICSERVER");

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        /*
        executorService.submit(() -> {
            try {
                base_test(url, false, true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        executorService.submit(() -> {
            try {
                base_test(url, true, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        */
        /*
        executorService.submit(() -> {
            try {
                sub_speed_test(url, "speed_test_topic");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        */

//        executorService.submit(() -> {
//            try {
//                pub_speed_test(url, "speed_test_topic");
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });

    }
}
