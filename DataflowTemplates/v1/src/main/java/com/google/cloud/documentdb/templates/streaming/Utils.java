package com.google.cloud.documentdb.templates.streaming;

import org.apache.beam.sdk.options.PipelineOptions;

public interface Utils extends PipelineOptions {
    String getProjectId();
    void setProjectId(String projectId);

    String getSubscriptionName();
    void setSubscriptionName(String subscriptionName);

    String getDatabaseId();
    void setDatabaseId(String databaseId);

    String getCollectionName();
    void setCollectionName(String collectionName);

    String getTypeMappingPath();
    void setTypeMappingPath(String typeMappingPath);
}

