package com.rocks.service.oto;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.rocks.constant.Constant;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class OssToMoss extends ObjectToObjectBase<OSSClient, AmazonS3Client> {
    private static final Logger logger = Logger.getLogger("OssToMoss.class");
    private final OssObjectService orgObjectService = new OssObjectService();
    private final MossObjectService targetObjectService = new MossObjectService();

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
        moveObject(summaryList, originalDir, targetBucket, targetDir);
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
                moveObject(summaryList, originalDir, targetBucket, targetDir);
                isTruncated = objectListing.isTruncated();

                for (String dir : objectListing.getCommonPrefixes()) {
                    listDir(orgBucket, originalDir, dir, targetBucket, targetDir);
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
    public void listDir(String orgBucket, String originalDir, String dir, String targetBucket, String targetDir) {
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
                moveObject(summaryList, originalDir, targetBucket, targetDir);
                for (String dirName : objectListing.getCommonPrefixes()) {
                    listDir(orgBucket, originalDir, dirName, targetBucket, targetDir);
                }
            } catch (Exception e) {
                logger.error(" listDir() failed. uuid:" + uuid + " orgBucket:" + orgBucket + " countNextMarker:" + countNextMarker, e);
            }
        }
    }

    private ObjectMetadata toS3ObjectMetadata(com.aliyun.oss.model.ObjectMetadata objectMetadata) {
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

    private S3Object toS3Object(OSSObject object) {
        ObjectMetadata objectMetadata = toS3ObjectMetadata(object.getObjectMetadata());
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(object.getObjectContent());
        s3Object.setObjectMetadata(objectMetadata);
        return s3Object;
    }

    /**
     * 迁移对象
     * @param summaryList 迁移对象
     * @param targetBucket 目的端桶名
     */
    private void moveObject(List<OSSObjectSummary> summaryList, String originalDir, String targetBucket, String targetDir) {
        ThreadPoolExecutor executor = getThreadPoolExecutor();
        if (summaryList != null && summaryList.size() > 0) {
            for (OSSObjectSummary obj : summaryList) {
                executor.execute(() -> {
                    if (!originalDir.equals(obj.getKey())) {
                        migrate(obj.getBucketName(), obj.getKey(), targetBucket, targetDir + obj.getKey().replaceFirst(originalDir, ""));
                        curFinishTaskCount.incrementAndGet();
                    }
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
    protected void doMigrate(String orgBucket, String orgKey, String targetBucket, String targetKey) {
        // 下载对象
        OSSObject object = orgObjectService.getObject(orgClient, orgBucket, orgKey);
        // 上传对象
        if (object != null) {
            targetObjectService.putObject(targetClient, targetBucket, targetKey, toS3Object(object));
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
    protected Long getObjectContentLength(String orgBucket, String key) throws Exception {
        return orgObjectService.getObjectContentLength(orgClient, orgBucket, key);
    }
}
