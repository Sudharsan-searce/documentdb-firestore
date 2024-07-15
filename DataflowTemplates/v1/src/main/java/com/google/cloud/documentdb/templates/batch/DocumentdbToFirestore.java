package com.google.cloud.documentdb.templates.batch;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

public class DocumentdbToFirestore {
	public static void main(String[] args) {
		// Create pipeline options
		Utils options = PipelineOptionsFactory.fromArgs(args).as(Utils.class);
		// Validate the options
		validateOptions(options);
		// Create the pipeline
		Pipeline pipeline = Pipeline.create(options);
		// Initialize with the Runtime Arguments
		String project_id = options.getprojectId();
		String database_name = options.getdatabase_name();
		String collection_name = options.getcollection_name();
		Integer batch_size = options.getbatch_Size();
		String typeMappingPath = options.getTypeMappingPath();
		String gcsPath = options.getGcsPath().toString();
		String failTopic = options.getFailTopic().toString();

		// Start the pipeline
		PCollection<String> data = pipeline.apply("Read From GCS", TextIO.read().from(gcsPath));

		PCollection<Long> recordCount = data.apply("Count Records", Count.globally());

		// Log the count (optional)
		recordCount.apply("Log Record Count", ParDo.of(new DoFn<Long, Void>() {
			@ProcessElement
			public void processElement(@Element Long count, OutputReceiver<Void> out) {
				System.out.println("Number of records read from GCS: " + count);
			}
		}));

		PCollection<String> results = data.apply("Write to Firestore",
				ParDo.of(new GcsToFirestore(project_id, database_name, collection_name, batch_size, typeMappingPath)));

		// Write the failed records to a GCS file
			results.apply("Write Failed Records to Pub/Sub", PubsubIO.writeStrings().to(failTopic));
		try {
			pipeline.run();
		} catch (Exception e) {
			System.err.println("Pipeline execution failed: " + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void validateOptions(Utils options) {
		if (options.getprojectId() == null) {
			throw new IllegalArgumentException("Project ID is required.");
		}
		if (options.getdatabase_name() == null) {
			throw new IllegalArgumentException("Database name is required.");
		}
		if (options.getcollection_name() == null) {
			throw new IllegalArgumentException("Collection name is required.");
		}
		if (options.getbatch_Size() == null) {
			throw new IllegalArgumentException("Batch size is required.");
		}
		if (options.getGcsPath() == null) {
			throw new IllegalArgumentException("GCS path is required.");
		}
		if (options.getTypeMappingPath() == null) {
			throw new IllegalArgumentException("GcsPath for TypeMapping is required.");
		}
		if (options.getFailTopic() == null) {
			throw new IllegalArgumentException("PubSub fail Topic is required.");
		}
	}
}
