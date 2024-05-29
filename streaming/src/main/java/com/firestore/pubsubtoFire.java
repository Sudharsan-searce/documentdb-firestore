package com.firestore;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.cloud.firestore.CollectionReference;
import com.google.gson.JsonSyntaxException;

public class pubsubtoFire {
    public static void main(String[] args) {
        Utils options = PipelineOptionsFactory.fromArgs(args).as(Utils.class); // Create pipeline options from the provided arguments
        Pipeline pipeline = Pipeline.create(options);                          // Initialize the pipeline with the created options
        
        String projectId=options.getProjectId().get();                         // Extract necessary configuration values from the options
        String databaseName=options.getDatabaseId().get();
        String collectionName=options.getCollectionName().get();
        String subscriptionName=options.getSubscriptionName().get();

        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + subscriptionName));    // Read messages from the specified Pub/Sub subscription

        messages.apply("ProcessAndSendToFirestore", ParDo.of(new ProcessAndSendToFirestoreFn(projectId,databaseName,collectionName)));    // Process each message and send it to Firestore
        pipeline.run();      // Run the pipeline
    }
}
