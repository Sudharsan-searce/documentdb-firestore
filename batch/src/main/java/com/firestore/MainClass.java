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
        params options = PipelineOptionsFactory.fromArgs(args).as(params.class);
         
         options.setRegion("us-east4");
         options.setDiskSizeGb(100);
         options.setWorkerMachineType("n2-standard-128");
         options.setNumWorkers(3);
         options.setRunner(org.apache.beam.runners.dataflow.DataflowRunner.class);
          
        
        
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        
        String project_id=options.getprojectId().get();
        String database_name=options.getdatabase_name().get();
        String collection_name=options.getcollection_name().get();
        String batch_size=options.getbatch_Size().get();


        PCollection<String> data= pipeline.apply("Read From GCS",TextIO.read().from(options.getGcsPath())); 
      
        data.apply("Write to Firestore", ParDo.of(new GcstoFirestore(project_id,database_name,collection_name,batch_size)));
            

        // Run the pipeline
        pipeline.run();
   
    }
   
}

