import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClockTickZooKeeper implements Runnable, Closeable {

    private String id;
    private CuratorFramework curator;


    private static final String LATCH_PATH = "/clockTick/leader";

    public ClockTickZooKeeper(String zookeeperConnectionString) {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(400, 3);
        curator = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        curator.start();

        Random random = new Random();
        id = random.nextInt() + "";


        System.out.println("ClockTickConstructor");
        try {
            curator.getZookeeperClient().blockUntilConnectedOrTimedOut();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        try (LeaderLatch leaderLatch = new LeaderLatch(curator, LATCH_PATH, id)) {
            leaderLatch.start();

            leaderLatch.await(400, TimeUnit.MILLISECONDS);
            Instant instant = Instant.now();
            if (leaderLatch.hasLeadership()) {
                ZooKeeper zk = curator.getZookeeperClient().getZooKeeper();

                Stat stat = curator.checkExists().forPath("/clockTick/second");
                long lastSecond = 0L;
                if (stat != null) {
                    byte[] bytes = curator.getData().forPath("/clockTick/second");
                    lastSecond = ByteBuffer.wrap(bytes).getLong();

                } else {
                    EnsurePath ensurePath = new EnsurePath("/clockTick/second");
                    ensurePath.ensure(curator.getZookeeperClient());
                }

                long nowEpochSecond = instant.getEpochSecond();
                if (lastSecond < nowEpochSecond) {
                    curator.setData().forPath("/clockTick/second", ByteBuffer.allocate(8).putLong(nowEpochSecond).array());
                    System.out.println("ClockTick: " + nowEpochSecond);
                }


            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void close() throws IOException {
        curator.close();
    }


    public static void main(String[] args) throws InterruptedException {
        String zookeeperConnectionString = "";//TODO

        ScheduledExecutorService clockTickExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(false).setNameFormat("CassandraScheduler-ClockTick-%d").build());
        clockTickExecutor.scheduleAtFixedRate(new ClockTickZooKeeper(zookeeperConnectionString), 0L, 200L, TimeUnit.MILLISECONDS);
    }


}
