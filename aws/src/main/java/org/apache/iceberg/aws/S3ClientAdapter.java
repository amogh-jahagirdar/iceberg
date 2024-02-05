/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class S3ClientAdapter implements S3Client {

  private final S3AsyncClient asyncClient;

  S3ClientAdapter(S3AsyncClient asyncClient) {
    this.asyncClient = asyncClient;
  }

  @Override
  public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest)
      throws AwsServiceException, SdkClientException, S3Exception {
    try {
      return asyncClient.deleteObject(deleteObjectRequest).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest)
      throws AwsServiceException, SdkClientException {
    try {
      return asyncClient
          .getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
          .get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ResponseInputStream<GetObjectResponse> getObject(
      GetObjectRequest getObjectRequest, ResponseTransformer responseTransformer)
      throws AwsServiceException, SdkClientException {
    try {
      return asyncClient
          .getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
          .get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) {
    try {
      return asyncClient.headObject(headObjectRequest).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request)
      throws NoSuchBucketException, AwsServiceException, SdkClientException, S3Exception {
    return S3Client.super.listObjectsV2(listObjectsV2Request);
  }

  @Override
  public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
      throws AwsServiceException, SdkClientException, S3Exception {
    return S3Client.super.putObject(putObjectRequest, requestBody);
  }

  @Override
  public PutObjectResponse putObject(PutObjectRequest putObjectRequest, Path sourcePath)
      throws AwsServiceException, SdkClientException, S3Exception {
    return S3Client.super.putObject(putObjectRequest, sourcePath);
  }

  @Override
  public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, RequestBody requestBody)
      throws AwsServiceException, SdkClientException, S3Exception {
    return S3Client.super.uploadPart(uploadPartRequest, requestBody);
  }

  @Override
  public String serviceName() {
    return null;
  }

  @Override
  public void close() {
    asyncClient.close();
  }
}
