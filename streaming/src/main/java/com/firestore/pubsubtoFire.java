package com.firestore;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


public class pubsubtoFire {
    public static void main(String[] args) {
        Utils options = PipelineOptionsFactory.fromArgs(args).as(Utils.class);
        Pipeline pipeline = Pipeline.create(options);
        
        String projectId=options.getProjectId().get();
        String databaseName=options.getDatabaseId().get();
        String collectionName=options.getCollectionName().get();
        String subscriptionName=options.getSubscriptionName().get();

        PCollection<String> messages = pipeline.apply("ReadFromPubSub",
                PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + subscriptionName));

        messages.apply("ProcessAndSendToFirestore", ParDo.of(new WriteToFirestore(projectId,databaseName,collectionName)));
        pipeline.run();
    }
}
