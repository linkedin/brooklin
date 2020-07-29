/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.committer;

import java.util.List;

import com.linkedin.datastream.cloud.storage.CommitCallback;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;

/**
 * Cloud Storage Committer interface that should be implemented to commit files to specific cloud provider storage.
 */
public interface ObjectCommitter {
     /**
      * Commits the file to cloud storage
      * @param filePath file that should be committed
      * @param fileFormat file format
      * @param destination destination bucket
      * @param topic source topic of the data in the file
      * @param partition source partition of the data in the file
      * @param minOffset minimum source offset of the data in the file
      * @param maxOffset maximum source offset of the data in the file
      * @param ackCallbacks list of call backs associated to each record in the file that should be called in order
      * @param recordMetadata metadata associated to each record in the file
      * @param callback callback that needs to be called to notify the Object Builder about the completion of the commit
      */
     void commit(String filePath,
                 String fileFormat,
                 String destination,
                 final String topic,
                 final long partition,
                 final long minOffset,
                 final long maxOffset,
                 List<SendCallback> ackCallbacks,
                 List<DatastreamRecordMetadata> recordMetadata,
                 CommitCallback callback);

     /**
      * Shutdown the committer
      */
     void shutdown();
}
