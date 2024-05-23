package com.psfirestore;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant; // Import Instant for current timestamp
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class data {
    public static void main(String... args) throws Exception {
        // TODO: Replace these variables before running the sample.
        String projectId = "cloudside-academy";
        String topicId = "livedata_bigquery";

        publishRandomData(projectId, topicId);
    }

    public static void publishRandomData(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException {
        TopicName topicName = TopicName.of(projectId, topicId);

        Publisher publisher = null;
        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

            // Run indefinitely
            while (true) {
                // Generate random ID
                int id = generateRandomId();

                // Generate random date
                String date = generateRandomDate();
                int min = 400000; // Minimum value (inclusive)
                int max = 1000000;
                int salary = getRandomIntInRange(min, max);
                
                // Generate current timestamp in the specified format
                String timestamp = getCurrentTimestamp();

                // Create JSON object
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("ID", id);
                jsonObject.addProperty("Date", date);
                jsonObject.addProperty("Annual salary", salary);
                jsonObject.addProperty("Timestamp", timestamp); // Add timestamp to JSON

                // Convert JSON object to string
                String jsonMessage = new Gson().toJson(jsonObject);

                // Create a Pub/Sub message
                ByteString data = ByteString.copyFromUtf8(jsonMessage);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Publish the message to the topic
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                //String messageId = messageIdFuture.get();
                System.out.println("Published message: " + jsonMessage);

                // Wait for a short time before publishing the next message
                Thread.sleep(15000); // Adjust the delay as needed
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }

    // Method to generate a random ID
    private static int generateRandomId() {
        Random random = new Random();
        return random.nextInt(10000); // Change 10000 to your desired ID range
    }

    // Method to generate a random date
    private static String generateRandomDate() {
        long offset = new Date().getTime();
        long end = offset + 365L * 24L * 60L * 60L * 1000L; // Add one year in milliseconds
        long diff = end - offset + 1;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(new Date(offset + (long) (Math.random() * diff)));
    }

    public static int getRandomIntInRange(int min, int max) {
        // Create a Random object
        Random random = new Random();
        // Generate a random integer within the specified range
        return random.nextInt((max - min) + 1) + min;
    }

    private static String getCurrentTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Kolkata")); // Set timezone to IST
        return sdf.format(new Date());
    }
}
