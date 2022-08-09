package com.rocks.service.oto;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import com.rocks.constant.Constant;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

public class OssObjectService implements IOSS<OSSClient, OSSClient, ObjectMetadata, OSSObject, PartETag> {
    private static final Logger logger = Logger.getLogger("OssObjectService.class");

    @Override
    public int getObjectCount(OSSClient orgClient, String orgBucket, String orgDir) {
        ObjectListing objectListing;
        boolean isTruncated = true;
        int totalNum = 0;
        String countNextMarker = "";
        List<OSSObjectSummary> summaryList;
        ListObjectsRequest request = new ListObjectsRequest(orgBucket, "", null, "/", 1000);
        request.setPrefix(orgDir);
        while (isTruncated) {
            try {
                request.setMarker(countNextMarker);
                objectListing = orgClient.listObjects(request);
                summaryList = objectListing.getObjectSummaries();
                countNextMarker = objectListing.getNextMarker();
                summaryList = summaryList.stream().filter(s -> !s.getKey().equals(orgDir)).collect(Collectors.toList());
                totalNum += summaryList.size();
                isTruncated = objectListing.isTruncated();

                for (String dir : objectListing.getCommonPrefixes()) {
                    totalNum = listDir(orgClient, orgBucket, dir, totalNum);
                }
            } catch (OSSException | ClientException e) {
                logger.error("listObjectsCountLoop error.", e);
            }
        }
        return totalNum;
    }

    @Override
    public boolean bucketExist(OSSClient client, String bucket) {
        return client.doesBucketExist(bucket);
    }

    @Override
    public ObjectMetadata headObject(OSSClient client, String bucket, String key) {
        return client.getObjectMetadata(bucket, key);
    }

    @Override
    public OSSObject getObject(OSSClient client, String bucket, String key) {
        return client.getObject(bucket, key);
    }

    @Override
    public OSSObject multiPartDownloadRange(OSSClient client, String bucket, String key, long start, long end, String uploadId) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, key);
        getObjectRequest.setRange(start, end);
        return client.getObject(getObjectRequest);
    }

    @Override
    public Long getObjectContentLength(OSSClient client, String bucket, String key) throws Exception {
        ObjectMetadata objectMetadata;
        try {
            objectMetadata = client.getObjectMetadata(bucket, key);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return objectMetadata.getContentLength();
    }

    @Override
    public void putObject(OSSClient client, String bucket, String obj, OSSObject object) {
        putObject(client, bucket, obj, object.getObjectContent(), object.getObjectMetadata());
    }

    @Override
    public void putObject(OSSClient client, String bucket, String obj, InputStream input, ObjectMetadata metadata) {
        PutObjectRequest request = new PutObjectRequest(bucket, obj, input, metadata);
        client.putObject(request);
    }

    @Override
    public void multiPartUpload(OSSClient client, String bucketName, String obj,
                                InputStream inputStream, String uploadId, List<PartETag> partETagList,
                                long curPartSize, int partNumber) {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(bucketName);
        uploadPartRequest.setKey(obj);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(inputStream);
        uploadPartRequest.setPartSize(curPartSize);
        uploadPartRequest.setPartNumber(partNumber);
        UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
        partETagList.add(uploadPartResult.getPartETag());
        logger.debug("multiPartUpload success. bucket:" + bucketName + " obj:" + obj + " curPartSize:" + curPartSize + " partNum:" + partNumber);
    }

    public int listDir(OSSClient orgClient, String orgBucket, String dir, int totalNum) {
        List<OSSObjectSummary> summaryList;
        ListObjectsRequest request = new ListObjectsRequest(orgBucket, dir, null, "/", 1000);
        boolean isTruncated = true;
        String countNextMarker = "";
        while (isTruncated) {
            try {
                request.setMarker(countNextMarker);
                ObjectListing objectListing = orgClient.listObjects(request);
                countNextMarker = objectListing.getNextMarker();
                summaryList = objectListing.getObjectSummaries();
                totalNum += summaryList.size();
                isTruncated = objectListing.isTruncated();

                while (true) {

                }
//                for (String dirName : objectListing.getCommonPrefixes()) {
//                    totalNum = listDir(orgClient, orgBucket, dirName, totalNum);
//                }
            } catch (OSSException | ClientException e) {
                logger.error("listObjectsCountLoop error.", e);
            }
        }
        return totalNum;
    }
}
