package com.rocks.service.oto;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.rocks.constant.Constant;
import com.rocks.utils.CountMapCache;
import com.rocks.utils.TaskConfig;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * moss 储存 迁移到 moss 储存
 * @author zhuang
 * @date 2021-10-14
 */
public class MossToMoss extends ObjectToObjectBase<AmazonS3Client, AmazonS3Client> {
    private static final Logger logger = Logger.getLogger("MossToMoss.class");
    private final MossObjectService orgObjectService = new MossObjectService();
    private final MossObjectService targetObjectService = orgObjectService;

    @Override
    protected int getObjectCount(String orgBucket, String dir) {
        return orgObjectService.getObjectCount(orgClient, orgBucket, dir);
    }

    /**
     * 获取源端的对象并上传到目的端
     */
    @Override
    protected void migrateBatch(String orgBucket, String targetBucket, String nextMarker, String originalDir, String targetDir) {
        ObjectListing objectListing;
        List<S3ObjectSummary> summaryList;
        boolean isTruncated = true;
        ListObjectsRequest request = new ListObjectsRequest();
        request.setEncodingType("null");
        request.setBucketName(orgBucket);
        request.setPrefix(originalDir);
        curFinishTaskCount.set(0);
        while (isTruncated) {
            logger.debug(uuid + " migrateBatch() truncated. orgBucket:" + orgBucket);
            try {
                TaskConfig.setUuidInfo(uuid, nextMarker, CountMapCache.getSuccessSize(uuid), CountMapCache.getFailedSize(uuid));
                request.setMarker(nextMarker);
                objectListing = orgClient.listObjects(request);
                nextMarker = objectListing.getNextMarker();
                summaryList = objectListing.getObjectSummaries();
                moveObject(summaryList, originalDir, targetBucket, targetDir);
                isTruncated = objectListing.isTruncated();

                int i = 0;
                while (curFinishTaskCount.get() != summaryList.size()) {
                    try {
                        if (i++ % 1800 == 0) {
                            logger.debug(uuid + " migrateBatch() sleep. orgBucket:" + orgBucket + " originalDir:" + originalDir + " nextMarker:" + nextMarker + " isTruncated:" + isTruncated);
                            logger.debug(uuid + " curFinishTaskCount: " + curFinishTaskCount.get() + "  summaryList: " + summaryList.size());
                            logger.debug(uuid + " executorInfo: " + uuidThreadPoolExecutorMap.get(uuid));
                        }
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
                curFinishTaskCount.set(0);
            } catch (Exception e) {
                logger.error(uuid + " migrateBatch() failed. orgBucket:" + orgBucket + " nextMarker:" + nextMarker, e);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
    }

    /**
     * 迁移对象
     *
     * @param orgBucket 源端桶名
     * @param targetBucket 目的端桶名
     * @param failedList 需要重新上传的文件
     */
    @Override
    public void moveObject(String orgBucket, String originalDir, String targetBucket, String targetDir, List<String> failedList) {
        List<S3ObjectSummary> summaryList = new ArrayList<>();
        S3ObjectSummary summaryObj;
        for (String key : failedList) {
            summaryObj = new S3ObjectSummary();
            summaryObj.setBucketName(orgBucket);
            summaryObj.setKey(key);
            summaryList.add(summaryObj);
        }
        moveObject(summaryList, originalDir, targetBucket, targetDir);
    }

    /**
     * 迁移对象
     * @param summaryList 迁移对象
     * @param targetBucket 目的端桶名
     */
    public void moveObject(List<S3ObjectSummary> summaryList, String originalDir, String targetBucket, String targetDir) {
        ThreadPoolExecutor executor = getThreadPoolExecutor();
        if (summaryList != null && summaryList.size() > 0) {
            for (S3ObjectSummary obj : summaryList) {
                executor.execute(() -> {
                    if (!originalDir.equals(obj.getKey())) {
                        migrate(obj.getBucketName(), obj.getKey(), targetBucket, targetDir + obj.getKey().replaceFirst(originalDir, ""));
                    }
                    curFinishTaskCount.incrementAndGet();
                });
            }
        }
    }

    @Override
    protected void multiPartMigrate(String orgBucket, String orgKey, String targetBucket, String targetKey, long total) throws Exception {
        long perSize = getMigratePartSize();
        long startPos;
        long endPos;
        long curPartSize;
        // 上传或者下载 分段是否全部成功
        boolean isSuccess = false;
        // 计算分段数量
        int partCount = (int) (total / perSize);
        if (total % perSize != 0) {
            partCount ++;
        }
        InitiateMultipartUploadRequest iniUploadRequest = new InitiateMultipartUploadRequest(targetBucket, targetKey);
        InitiateMultipartUploadResult iniUploadResult = targetClient.initiateMultipartUpload(iniUploadRequest);
        String uploadId = iniUploadResult.getUploadId();
        List<PartETag> partETagList = new ArrayList<>();
        logger.info(uuid + " multiPartMigrate() orgBucket: " + orgBucket + " key:" + orgKey + "  targetKey:" + targetKey + "  total:" + total);
        for (int i = 0; i < partCount; i++) {
            startPos = i * perSize;
            endPos = (i + 1 == partCount) ? total : startPos + perSize;
            curPartSize = (i + 1 == partCount) ? total - startPos : perSize;
            isSuccess = false;
            for (int k=0; k++<Constant.RETRY_COUNT; ) {
                // 分段迁移的分段下载
                try (InputStream input = orgObjectService.multiPartDownloadRange(orgClient, orgBucket, orgKey,
                        startPos, endPos - 1, uploadId).getObjectContent()) {
                    logger.debug(uuid + " multiPartDownloadRange success. bucket:" + orgBucket + " orgKey:" + orgKey
                            + " startPos:" + startPos + " endPos:" + (endPos - 1) + " uploadId:" + uploadId);
                    // 分段迁移的分段上传
                    targetObjectService.multiPartUpload(targetClient, targetBucket, targetKey, input, uploadId, partETagList,
                            curPartSize, i + 1);
                    isSuccess = true;
                    logger.debug(uuid + " multiPartUpload success. bucket:" + targetBucket + " targetKey:" + targetKey
                            + " curPartSize:" + curPartSize + " partNum:" + (i + 1));
                    break;
                } catch (Exception e) {
                    logger.error(uuid + " multiPartUpload failed. bucket:" + targetBucket + " targetKey:" + targetKey
                            + " curPartSize:" + curPartSize + " partNum:" + (i + 1), e);
                }
            }
            // 上传重试失败
            if (!isSuccess) {
                logger.error(uuid + " multiPartMigrate() multiPartUpload retry failed.  bucket:" + targetBucket + " targetKey:" + targetKey
                        + " curPartSize:" + curPartSize + " partNum:" + (i + 1));
                break;
            }
        }

        // 分段上传或分段下载未成功
        if (!isSuccess) {
            try {
                // 删除已上传的数据
                targetClient.abortMultipartUpload(new AbortMultipartUploadRequest(targetBucket, targetKey, uploadId));
            } catch (Exception e1) {
                logger.error(uuid + " abortMultipartUpload error. targetBucket:" + targetBucket + " targetKey: " + targetKey, e1);
            }
            throw new Exception(uuid + ":multiPartMigrate() failed.");
        } else {
            try {
                // 完成分段上传
                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucket, targetKey, uploadId, partETagList);
                targetClient.completeMultipartUpload(completeRequest);
                logger.debug(uuid + " CompleteMultipartUpload success. targetKey:" + targetKey);
            } catch (Exception e) {
                logger.debug(uuid + " CompleteMultipartUpload error. "  + targetBucket + " : " + targetKey);
                try {
                    targetClient.abortMultipartUpload(new AbortMultipartUploadRequest(targetBucket, targetKey, uploadId));
                } catch (Exception e1) {
                    logger.error(uuid + " :multiPartMigrate() " + targetBucket + " : " + targetKey + " abortMultipartUpload error.", e1);
                }
                throw e;
            }
        }
    }

    @Override
    protected void doMigrate(String orgBucket, String orgKey, String targetBucket, String targetKey)  {
        S3Object object = orgObjectService.getObject(orgClient, orgBucket, orgKey);
        targetObjectService.putObject(targetClient, targetBucket, targetKey, object);
    }

    @Override
    public void shutdownClient() {
        orgClient.shutdown();
        targetClient.shutdown();
    }

    @Override
    protected Long getObjectContentLength(String orgBucket, String key) throws SdkClientException {
        return orgObjectService.getObjectContentLength(orgClient, orgBucket, key);
    }

}
