package com.rocks.service.oto;

import com.amazonaws.services.s3.AmazonS3Client;
import com.rocks.constant.Constant;
import com.rocks.constant.ErrorType;
import com.rocks.utils.CountMapCache;
import com.rocks.utils.CurFailedDocsCache;
import com.rocks.utils.FailedDocsCache;
import com.rocks.utils.MigrateUtils;
import com.rocks.vo.ClientEntityMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 对象储存迁移到对象储存
 * @author zhuang
 * @date 2021-10-14
 */
@Setter
@Getter
public abstract class ObjectToObjectBase<O, T> {
    private static Logger logger = Logger.getLogger("ObjectToObjectBase.class");
    /** 执行线程数 */
    private Integer workNum = Constant.THREAD_SIZE;
    private long migrateMaxSize = Constant.MIGRATE_MAX_SIZE;
    private long migratePartSize = Constant.MIGRATE_PART_SIZE;
    private long startTime;
    protected String uuid;
    protected O orgClient;
    protected T targetClient;
    protected ConcurrentHashMap<String, ThreadPoolExecutor> uuidThreadPoolExecutorMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, AmazonS3Client> uuidAmazonS3ClientMap = new ConcurrentHashMap<>();

    /** 当前子任务完成数量 */
    protected AtomicInteger curFinishTaskCount = new AtomicInteger(0);

    /**
     * 重试迁移失败的文件
     * @param failedList 需要重新迁移的文件
     */
    abstract void moveObject(String orgBucket, String originalDir, String targetBucket, String targetDir, List<String> failedList);

    /**
     * 获取源端的对象并上传到目的端
     */
    abstract void migrateBatch(String orgBucket, String targetBucket, String nextMarker, String originalDir, String targetDir);

    /**
     * 统计该桶内的文件数量
     * @param orgBucket 桶名
     */
    abstract int getObjectCount(String orgBucket, String dir);

    /** 关闭服务端 */
    public abstract void shutdownClient();

    /** 获取对象大小 */
    protected abstract Long getObjectContentLength(String orgBucket, String key) throws Exception;

    /**
     * 分段迁移
     */
    protected abstract void multiPartMigrate(String orgBucket, String orgKey, String targetBucket, String targetDir, long length) throws Exception;

    /**
     * 普通迁移
     */
    protected abstract void doMigrate(String orgBucket, String orgKey, String targetBucket, String targetDir);

    /**
     * 对象存储的桶内对象 迁移到 某个对象存储的桶内
     * @param orgBucket 源端桶名
     * @param targetBucket 目的端桶名
     * @param failedList 迁移失败的文件名称
     * @param uuid 事件标识
     */
    public void start(String orgBucket, String targetBucket, List<String> failedList, String marker, String uuid, String originalDir, String targetDir) {
        startTime = System.currentTimeMillis();
        this.uuid = uuid;
        if (null == failedList) {
            logger.info(uuid + " migrate processing...");
            // 新建下载
            // 异步计算文件总数
            ExecutorService asynPool = Executors.newFixedThreadPool(2);
            UploadCountTask uploadCountTask = new UploadCountTask(orgBucket, uuid, originalDir);
            Future<Integer> future = asynPool.submit(uploadCountTask);
            // 开始迁移
            asynPool.execute(() -> {
                try {
                    migrateBatch(orgBucket, targetBucket, marker, originalDir, targetDir);
                } catch (Exception e) {
                    logger.error(uuid + " start() migrateBatch failed. uuid:" + uuid + " orgBucket:" + orgBucket + " targetBucket:" + targetBucket, e);
                } finally {
                    logger.debug(uuid + " migrate end ");
                    shutdownExecutorAndClient(uuid);
                }
            });
            // 获取异步计算结果
            try {
                while(true) {
                    if (future.isDone()) {
                        // 缓存map记录文件总数
                        logger.info(uuid + " count total num " + future.get() + " success.");
                        CountMapCache.addTotalNumToMap(uuid, future.get());
                        break;
                    }
                    TimeUnit.SECONDS.sleep(1);
                }
            } catch (Exception e) {
                logger.error(uuid + " Asyn Count error.", e);
            }
        } else {
            logger.info(uuid + " migrate retry processing...");
            // 重试下载
            try {
                moveObject(orgBucket, originalDir, targetBucket, targetDir, failedList);
            } catch (Exception e) {
                logger.error(uuid + " start() moveObject failed. uuid:" + uuid + " orgBucket:" + orgBucket + " targetBucket:" + targetBucket, e);
            } finally {
                shutdownExecutorAndClient(uuid);
            }
        }
    }

    /**
     * 迁移：分段迁移、普通迁移
     */
    public void migrate(String orgBucket, String orgKey, String targetBucket, String targetKey) {
        try {
            Long length = getObjectContentLength(orgBucket, orgKey);
            if (length >= migrateMaxSize) {
                multiPartMigrate(orgBucket, orgKey, targetBucket, targetKey, length);
            } else {
                doMigrate(orgBucket, orgKey, targetBucket, targetKey);
            }
            CountMapCache.countNum(uuid, "s");
        } catch (Exception e) {
            logger.error(uuid + " migrate object error. key: " + orgKey, e);
            // 失败计数和失败文件名、原因记录
            int errorType = ErrorType.getErrorType(e.getMessage());
            CountMapCache.countNum(uuid, "f");
//            FailedDocsCache.addFailedDocs(uuid, orgKey, errorType);
            MigrateUtils.storeFailedDocs(uuid, orgKey, errorType);
            CurFailedDocsCache.addFailedDocs(uuid, orgKey, errorType);
        }
    }

    /**
     * 获取线程池
     */
    ThreadPoolExecutor getThreadPoolExecutor() {
        ThreadPoolExecutor threadPoolExecutor = uuidThreadPoolExecutorMap.get(uuid);
        if (threadPoolExecutor == null) {
            threadPoolExecutor = new ThreadPoolExecutor(workNum, workNum, 3000L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(100));
            threadPoolExecutor.setRejectedExecutionHandler((r, executor) -> {
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RejectedExecutionException("downloadExecutor Producer interrupted", e);
                }
            });
            uuidThreadPoolExecutorMap.put(uuid, threadPoolExecutor);
        }
        return threadPoolExecutor;
    }

    /**
     * 关闭线程池和服务端
     */
     void shutdownExecutorAndClient(String uuid)  {
         // 关闭线程池
         ThreadPoolExecutor executor = uuidThreadPoolExecutorMap.get(uuid);

         while (true){
             logger.debug(uuid + " : executor " + executor);
             if (executor == null) {
                 executor = uuidThreadPoolExecutorMap.get(uuid);
             }
             if (executor != null && (executor.getActiveCount() == 0) && (executor.getCompletedTaskCount() == executor.getTaskCount())) {
                 logger.info(uuid + " shutdown. curTaskCount:" + executor.getCompletedTaskCount());
                 executor.shutdown();
                 break;
             }
             try {
                 TimeUnit.SECONDS.sleep(1);
             } catch (InterruptedException e) {
                 e.printStackTrace();
             }
         }

         while (true){
             if (executor.isTerminated()){
                 uuidThreadPoolExecutorMap.remove(uuid);
                 logger.info(uuid + " migrate end, costs: " + (System.currentTimeMillis() - startTime) + "ms.");

                 while (true) {
                     // 等待计算总数任务结束，防止迁移完成，还处于计算中
                     if (CountMapCache.getInstance().getCache(uuid).get("t") != null) {
                         // 对计数结果进行处理，防止任务不结束情况, e值不为0则底层结束迁移
                         CountMapCache.countNum(uuid, "e");
                         break;
                     }
                     try {
                         TimeUnit.SECONDS.sleep(1);
                     } catch (Exception e) {
                         e.printStackTrace();
                     }
                 }

                 break;
             } else {
                 logger.info(uuid + " ecutor is not Terminated");
             }
             try {
                 TimeUnit.SECONDS.sleep(1);
             } catch (InterruptedException e) {
                 e.printStackTrace();
             }
         }
         // 关闭服务端
         shutdownClient();
         // 清除ClientEntityMap的该uuid信息
         ClientEntityMap.remove(uuid);
         CurFailedDocsCache.removeCache(uuid);
    }

    /**
     * 上传异步计算总数内部类
     */
    class UploadCountTask implements Callable<Integer> {
        String uuid;
        String bucket;
        String dir;
        UploadCountTask(String bucket, String uuid, String dir) {
            this.bucket = bucket;
            this.uuid = uuid;
            this.dir = dir;
        }

        @Override
        public Integer call() {
            // 计算文件总数
            long migrateStartTime = System.currentTimeMillis();
            int totalNum;
            while (true) {
                try {
                    totalNum = getObjectCount(bucket, dir);
                    logger.info(uuid + " migrate task total num: " + totalNum + ", count costs " + (System.currentTimeMillis() - migrateStartTime) + "ms");
                    break;
                } catch (Exception e) {
                    logger.info(uuid + " migrate task total num failed.count costs " + (System.currentTimeMillis() - migrateStartTime) + "ms", e);
                }

                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return totalNum;
        }
    }

    /**
     * 关闭目的端client
     */
    public void closeTargetClient(String uuid) {
        logger.info(uuid + " closeTargetClient start.");
        AmazonS3Client amazonS3Client = uuidAmazonS3ClientMap.get(uuid);
        if (amazonS3Client != null) {
            amazonS3Client.shutdown();
            logger.info(uuid + " closeTargetClient success.");
        } else {
            logger.info(uuid + " closeTargetClient failed.");
        }
    }
}
