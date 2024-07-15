package com.google.cloud.documentdb.templates.streaming;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PubSubReader {

    public static class ReadFromPubSubFn extends DoFn<Long, PubsubMessage> {
        private final String projectId;
        private final String subscriptionId;
        private transient Subscriber subscriber;
        private transient BlockingQueue<PubsubMessage> messages;

        public ReadFromPubSubFn(String projectId, String subscriptionId) {
            this.projectId = projectId;
            this.subscriptionId = subscriptionId;
        }

        @Setup
        public void setup() {
            try {
                messages = new LinkedBlockingQueue<>();
                ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

                MessageReceiver receiver = (message, consumer) -> {
                    messages.offer(message);
                    consumer.ack();
                };

                subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
                subscriber.startAsync().awaitRunning();
            } catch (Exception e) {
                System.err.println("Error during setup: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Teardown
        public void teardown() {
            try {
                if (subscriber != null) {
                    subscriber.stopAsync();
                }
            } catch (Exception e) {
                System.err.println("Error during teardown: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                PubsubMessage message = messages.poll();

                if (message != null) {
                    c.output(message);
                }
            } catch (Exception e) {
                System.err.println("Error during message processing: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
