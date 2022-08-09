package com.rocks.service.oto;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.*;
import com.rocks.constant.Constant;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * oss 储存 迁移到 oss 储存
 * @author zhuang
 */
public class OssToOss extends ObjectToObjectBase<OSSClient, OSSClient> {
    private static final Logger logger = Logger.getLogger("OssToOss.class");
    private final OssObjectService orgObjectService = new OssObjectService();
    private final OssObjectService targetObjectService = orgObjectService;

    @Override
    void moveObject(String orgBucket, String originalDir, String targetBucket, String targetDir, List<String> failedList) {
        List<OSSObjectSummary> summaryList = new ArrayList<>();
        OSSObjectSummary summaryObj;
        for (String key : failedList) {
            summaryObj = new OSSObjectSummary();
            summaryObj.setBucketName(orgBucket);
            summaryObj.setKey(key);
            summaryList.add(summaryObj);
        }
        moveObject(summaryList, targetBucket, targetDir);
    }

    @Override
    void migrateBatch(String orgBucket, String targetBucket, String nextMarker, String originalDir, String targetDir) {
        ObjectListing objectListing;
        List<OSSObjectSummary> summaryList;
        nextMarker = "".equals(nextMarker) ? null : nextMarker;
        boolean isTruncated = true;
        ListObjectsRequest request = new ListObjectsRequest(orgBucket, "", null, "/", 1000);
        request.setPrefix(originalDir);
        while (isTruncated) {
            try {
                request.setMarker(nextMarker);
                objectListing = orgClient.listObjects(request);
                nextMarker = objectListing.getNextMarker();
                summaryList = objectListing.getObjectSummaries();
                moveObject(summaryList, targetBucket, "");
                isTruncated = objectListing.isTruncated();

                for (String dir : objectListing.getCommonPrefixes()) {
                    listDir(orgBucket, dir, targetBucket);
                }
            } catch (Exception e) {
                logger.error(" migrateBatch() failed. uuid:" + uuid + " orgBucket:" + orgBucket + " nextMarker:" + nextMarker, e);
            }
        }
    }

    /**
     * 遍历该目录名称下的所有对象，并进行迁移
     * @param orgBucket 桶名
     * @param dir 目录名称
     */
    public void listDir(String orgBucket, String dir, String targetBucket) {
        List<OSSObjectSummary> summaryList;
        ListObjectsRequest request = new ListObjectsRequest(orgBucket, dir, null, "/", 1000);
        boolean isTruncated = true;
        String countNextMarker = null;
        while (isTruncated) {
            try {
                request.setMarker(countNextMarker);
                ObjectListing objectListing = orgClient.listObjects(request);
                countNextMarker = objectListing.getNextMarker();
                summaryList = objectListing.getObjectSummaries();
                isTruncated = objectListing.isTruncated();
                moveObject(summaryList, targetBucket, "");
                for (String dirName : objectListing.getCommonPrefixes()) {
                    listDir(orgBucket, dirName, targetBucket);
                }
            } catch (Exception e) {
                logger.error(" listDir() failed. uuid:" + uuid + " orgBucket:" + orgBucket + " countNextMarker:" + countNextMarker, e);
            }
        }
    }

    private void moveObject(List<OSSObjectSummary> summaryList, String targetBucket, String targetDir) {
        ThreadPoolExecutor executor = getThreadPoolExecutor();
        if (summaryList != null && summaryList.size() > 0) {
            for (OSSObjectSummary obj : summaryList) {
                executor.execute(() -> migrate(obj.getBucketName(), obj.getKey(), targetBucket, targetDir + obj.getKey()));
            }
        }
    }

    @Override
    int getObjectCount(String orgBucket, String dir) {
        return orgObjectService.getObjectCount(orgClient, orgBucket, dir);
    }

    @Override
    public void shutdownClient() {
        orgClient.shutdown();
        targetClient.shutdown();
    }

    @Override
    protected void multiPartMigrate(String orgBucket, String orgKey, String targetBucket, String targetKey, long total) throws Exception {
        long perSize = getMigratePartSize();
        long startPos;
        long endPos;
        long curPartSize;
        // 上传或者下载的分段操作是否全部成功
        boolean isSuccess = false;
        // 计算分段数量
        int partCount = (int) (total / perSize);
        if (total % perSize != 0) {
            partCount ++;
        }
        InitiateMultipartUploadRequest iniUploadRequest = new InitiateMultipartUploadRequest(targetBucket, orgKey);
        InitiateMultipartUploadResult iniUploadResult = targetClient.initiateMultipartUpload(iniUploadRequest);
        String uploadId = iniUploadResult.getUploadId();
        List<PartETag> partETagList = new ArrayList<>();
        logger.debug("multiPartMigrate() orgBucket: " + orgBucket + " key:" + orgKey + "  total:" + total);
        for (int i = 0; i < partCount; i++) {
            startPos = i * perSize;
            endPos = (i + 1 == partCount) ? total : startPos + perSize;
            curPartSize = (i + 1 == partCount) ? total - startPos : perSize;

            for (int k=0; k++<Constant.RETRY_COUNT; ) {
                // 分段迁移的分段下载
                try (InputStream input = orgObjectService.multiPartDownloadRange(orgClient, orgBucket, orgKey,
                        startPos, endPos - 1, uploadId).getObjectContent()) {
                    // 分段迁移的分段上传
                    targetObjectService.multiPartUpload(targetClient, targetBucket, orgKey, input, uploadId, partETagList,
                            curPartSize, i + 1);
                    isSuccess = true;
                    logger.info("multiPartUpload success. bucket:" + targetBucket + " obj:" + orgKey
                            + " curPartSize:" + curPartSize + " partNum:" + (i + 1));
                    break;
                } catch (Exception e) {
                    logger.error("multiPartUpload failed. bucket:" + targetBucket + " obj:" + orgKey
                            + " curPartSize:" + curPartSize + " partNum:" + (i + 1), e);
                }
            }
            // 上传重试失败
            if (!isSuccess) {
                logger.error("multiPartMigrate() multiPartUpload retry failed.  bucket:" + targetBucket + " obj:" + orgKey
                        + " curPartSize:" + curPartSize + " partNum:" + (i + 1));
                break;
            }
        }

        // 分段上传或分段下载未成功
        if (!isSuccess) {
            try {
                // 删除已上传的数据
                targetClient.abortMultipartUpload(new AbortMultipartUploadRequest(targetBucket, orgKey, uploadId));
            } catch (Exception e1) {
                logger.error(targetBucket + " : " + orgKey + " abortMultipartUpload error.", e1);
            }
            throw new Exception("multiPartMigrate() failed.");
        } else {
            try {
                // 完成分段上传
                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucket, orgKey, uploadId, partETagList);
                targetClient.completeMultipartUpload(completeRequest);
            } catch (Exception e) {
                try {
                    targetClient.abortMultipartUpload(new AbortMultipartUploadRequest(targetBucket, orgKey, uploadId));
                } catch (Exception e1) {
                    logger.error(targetBucket + " : " + orgKey + " abortMultipartUpload error.", e1);
                }
                throw e;
            }
        }
    }

    @Override
    protected void doMigrate(String orgBucket, String orgKey, String targetBucket, String targetKey) {
        OSSObject object = orgObjectService.getObject(orgClient, orgBucket, orgKey);
        targetObjectService.putObject(targetClient, targetBucket, targetKey, object);
    }

    @Override
    protected Long getObjectContentLength(String orgBucket, String key) throws Exception {
        return orgObjectService.getObjectContentLength(orgClient, orgBucket, key);
    }
}
