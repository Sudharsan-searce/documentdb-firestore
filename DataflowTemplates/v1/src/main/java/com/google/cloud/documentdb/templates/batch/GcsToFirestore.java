package com.google.cloud.documentdb.templates.batch;

import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import java.io.IOException;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.DocumentReference;
import com.google.gson.Gson;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import java.util.concurrent.ExecutionException;
import com.google.gson.reflect.TypeToken;

public class GcsToFirestore extends DoFn<String, String> {
	private final String project_id, db_name, collection_name, typeMappingPath;
	private final Integer batch_size;
	Pipeline pipeline;
	private final TupleTag<String> failureTag = new TupleTag<String>() {
	};
	Map<String, Object> getOid;

	public GcsToFirestore(String project_id, String db_name, String collection_name, Integer batch_size,
			String typeMappingPath) {
		this.project_id = project_id;
		this.db_name = db_name;
		this.collection_name = collection_name;
		this.batch_size = batch_size;
		this.typeMappingPath = typeMappingPath;
	}

	@ProcessElement
	public void processElement(@Element String c, OutputReceiver<String> out) throws Exception {
		// Initialize Firestore

		Firestore firestore = null;
		try {
			firestore = FirestoreOptions.getDefaultInstance().toBuilder().setProjectId(project_id)
					.setDatabaseId(db_name).build().getService();

			if (firestore == null) {
				System.err.println("Failed to initialize Firestore. Firestore is null.");
				return;
			}
			CollectionReference collection = firestore.collection(collection_name);
			TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {
			};
			Gson gson = new Gson();
			TypeCasting typecasting = new TypeCasting();
			List<QueryDocumentSnapshot> documentId = null;
			List<Map<String, Object>> jsonLines = new ArrayList<>();
			// Output from previous pipeline
			String data = null;
			try {
				data = c;
				JSONObject jsonData = new JSONObject(data);
				Map<String, Object> event = typecasting.convertDataType(jsonData, typeMappingPath);
				Map<String, Object> locateId = gson.fromJson(data, typeToken.getType());
				Map<String, Object> getOid = (Map<String, Object>) locateId.get("_id");
				// append the input to the list
				jsonLines.add(event);
				// batch_processing
				do {
					processChunk(jsonLines, collection, gson);
					ApiFuture<QuerySnapshot> query = collection.whereEqualTo("_id.$oid", getOid.get("$oid").toString())
							.get();
					documentId = query.get().getDocuments();
				} while (documentId.size() == 0);
			} catch (Exception e) {
				StringBuilder errorRecord = new StringBuilder();
				errorRecord.append("{");
				errorRecord.append("\"ingestionType\": \"batch\",");
				errorRecord.append("\"collectionName\": \"").append(collection_name).append("\",");
				errorRecord.append("\"data\": ").append(JSONObject.quote(data)).append(",");
				errorRecord.append("\"errorMessage\": \"").append(e.getMessage()).append("\"");
				errorRecord.append("}");
				String errorData=errorRecord.toString();
				out.output(errorData);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (firestore != null) {
				firestore.shutdown();
				firestore.close();
			}
		}
	}

	private void processChunk(List<Map<String, Object>> batch, CollectionReference collection, Gson gson)
			throws IOException, InterruptedException, ExecutionException {
		WriteBatch writeBatch = collection.getFirestore().batch();

		for (Map<String, Object> json : batch) {
			DocumentReference docRef = collection.document();
			Thread.sleep(10);
			writeBatch.set(docRef, json);
		}

		try {
			writeBatch.commit().get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
}