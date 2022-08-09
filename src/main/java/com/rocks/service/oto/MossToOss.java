package com.rocks.service.oto;


import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.*;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.rocks.constant.Constant;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Deprecated
public class MossToOss extends ObjectToObjectBase<AmazonS3, OSSClient> {
    private static final Logger logger = Logger.getLogger("MossToOss.class");
    private final MossObjectService orgObjectService = new MossObjectService();
    private final OssObjectService targetObjectService = new OssObjectService();

    @Override
    void moveObject(String orgBucket, String originalDir, String targetBucket, String targetDir, List<String> failedList) {
        List<S3ObjectSummary> summaryList = new ArrayList<>();
        S3ObjectSummary summaryObj;
        for (String key : failedList) {
            summaryObj = new S3ObjectSummary();
            summaryObj.setBucketName(orgBucket);
            summaryObj.setKey(key);
            summaryList.add(summaryObj);
        }
        moveObject(summaryList, targetBucket, targetDir);
    }

    @Override
    void migrateBatch(String orgBucket, String targetBucket, String nextMarker, String originalDir, String targetDir) {
        ObjectListing objectListing;
        List<S3ObjectSummary> summaryList;
        boolean isTruncated = true;
        ListObjectsRequest request = new ListObjectsRequest();
        request.setEncodingType("null");
        request.setBucketName(orgBucket);
        request.setPrefix(originalDir);
        while (isTruncated) {
            try {
                request.setMarker(nextMarker);
                objectListing = orgClient.listObjects(request);
                nextMarker = objectListing.getNextMarker();
                summaryList = objectListing.getObjectSummaries();
                moveObject(summaryList, targetBucket, targetDir);
                isTruncated = objectListing.isTruncated();
            } catch (Exception e) {
                logger.error(" migrateBatch() failed. uuid:" + uuid + " orgBucket:" + orgBucket + " nextMarker:" + nextMarker, e);
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
     * @param summaryList 需要重试的对象
     * @param targetBucket 目的端桶名
     */
    public void moveObject(List<S3ObjectSummary> summaryList, String targetBucket, String targetDir) {
        ThreadPoolExecutor executor = getThreadPoolExecutor();
        if (summaryList != null && summaryList.size() > 0) {
            for (S3ObjectSummary obj : summaryList) {
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


    private ObjectMetadata toOssObjetMetadata(com.amazonaws.services.s3.model.ObjectMetadata objectMetadata) {
        ObjectMetadata metadata = new ObjectMetadata();
        Map<String, Object> rawMetadata = objectMetadata.getRawMetadata();
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        Object value;
        for (String key : rawMetadata.keySet()) {
            value = rawMetadata.get(key);
            metadata.setHeader(key, value);
        }
        for (String key : userMetadata.keySet()) {
            metadata.addUserMetadata(key, userMetadata.get(key));
        }
        return metadata;
    }

    private OSSObject toOssObject(com.amazonaws.services.s3.model.S3Object s3Object) {
        ObjectMetadata objectMetadata = toOssObjetMetadata(s3Object.getObjectMetadata());
        OSSObject ossObject = new OSSObject();
        ossObject.setObjectContent(s3Object.getObjectContent());
        ossObject.setObjectMetadata(objectMetadata);
        return ossObject;
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
                    logger.debug("multiPartUpload success. bucket:" + targetBucket + " obj:" + orgKey
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
        // 下载对象
        S3Object s3Object = orgObjectService.getObject(orgClient, orgBucket, orgKey);
        // 上传对象
        if (s3Object != null) {
            targetObjectService.putObject(targetClient, targetBucket, targetKey, toOssObject(s3Object));
        }
    }

    @Override
    protected Long getObjectContentLength(String orgBucket, String key) throws SdkClientException {
        return orgObjectService.getObjectContentLength(orgClient, orgBucket, key);
    }
}
