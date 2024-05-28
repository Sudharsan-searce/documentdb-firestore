package com.firestore;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface Utils extends PipelineOptions {
    ValueProvider<String> getProjectId();
    void setProjectId(ValueProvider<String> projectId);

    ValueProvider<String> getSubscriptionName();
    void setSubscriptionName(ValueProvider<String> subscriptionName);

    ValueProvider<String> getDatabaseId();
    void setDatabaseId(ValueProvider<String> databaseId);

    ValueProvider<String> getCollectionName();
    void setCollectionName(ValueProvider<String> collectionName);
}
