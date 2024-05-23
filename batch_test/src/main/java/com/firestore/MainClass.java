package com.firestore;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;


public class MainClass {
       public static void main(String[] args) {
        // Create pipeline options
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
         options.setProject("gcp-firestore-423907");
         options.setRegion("us-east4");
         options.setDiskSizeGb(100);
         options.setWorkerMachineType("n2-standard-128");
         options.setNumWorkers(3);
         options.setRunner(org.apache.beam.runners.dataflow.DataflowRunner.class);
          
        
         String gcsPath = "gs://gcp-firestore-storage/MongoDB_dumps/meta.json";
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        
        
        PCollection<String> data= pipeline.apply(TextIO.read().from(gcsPath));
      
        data.apply("Read JSON from GCS and Store in Firestore", ParDo.of(new GcstoFirestore()));
            

        // Run the pipeline
        pipeline.run();
   
    }
   
}

