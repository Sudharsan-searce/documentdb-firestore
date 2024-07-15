package com.google.cloud.documentdb.templates.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import com.google.pubsub.v1.PubsubMessage;

public class DocumentdbToFirestore {
    public static void main(String[] args) {
        Utils options = PipelineOptionsFactory.fromArgs(args).as(Utils.class);
        validateOptions(options);

        String projectId = options.getProjectId();
		String databaseName = options.getDatabaseId();
		String collectionName = options.getCollectionName();
		String subscriptionName = options.getSubscriptionName();
		String typeMappingPath=options.getTypeMappingPath();

        Pipeline pipeline = Pipeline.create(options);
        PCollection<PubsubMessage> pubsubMessages = pipeline
            .apply("GenerateSequence", GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
            .apply("ReadFromPubSub", ParDo.of(new PubSubReader.ReadFromPubSubFn(projectId, subscriptionName)));
        pubsubMessages.apply("Write To Firestore",ParDo.of(new PubsubToFirestore(projectId, databaseName, collectionName,typeMappingPath)));
        try {
			pipeline.run();
		} catch (Exception e) {
			System.err.println("Pipeline execution failed: " + e.getMessage());
			e.printStackTrace();
		}
    }
    	private static void validateOptions(Utils options) {
		if (options.getProjectId() == null) {
			throw new IllegalArgumentException("Project ID is required.");
		}
		if (options.getDatabaseId() == null) {
			throw new IllegalArgumentException("Database name is required.");
		}
		if (options.getCollectionName() == null) {
			throw new IllegalArgumentException("Collection name is required.");
		}
		if (options.getSubscriptionName() == null) {
			throw new IllegalArgumentException("Batch size is required.");
		}
		if (options.getTypeMappingPath() == null) {
			throw new IllegalArgumentException("GcsPath for TypeMapping is required.");
		}
	}
}

