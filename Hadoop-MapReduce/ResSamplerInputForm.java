/** sk7970_sabarnikundu_question1

So this is file or peice of code sets the custom record reader and hence configures values including Number of Samples and maximum records
  
If we set number of samples to 1 then the record reader will read 1 random line from each file.
Reference: github,google
  */




package edu.nyu.tandon.bigdata.hadoop;

 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.io.*;
 import org.apache.hadoop.mapreduce.*;
 import org.apache.hadoop.util.ReflectionUtils;
 import java.io.IOException;
 import java.util.*;


 
public class ReservoirSamplerInputFormat<K extends Writable, V extends Writable>
        extends InputFormat {


    private InputForm<K, V> inputForm;

   //Configuring the input format
    public static void setInputFormat(Job job, Class<? extends InputForm> inputForm) {
        job.getConfiguration().setClass("reservoir.inputformat.class", inputForm,InputForm.class);
        job.setInputFormatClass(ReservoirSamplerInputFormat.class);
    }


    public static void setnumsamp(Job job, int Samples) {
        job.getConfiguration().setInt(S"reservoir.samples.number", Samples);
    }


    public static void setMaxRecordsToRead(Job job, int Records) {
        job.getConfiguration().setInt("reservoir.samples.maxrecordsread", Records);
    }



    public static void setUseSamplesNumberPerInputSplit(Job job, boolean usePerInputSplit) {
        job.getConfiguration().setBoolean(
                false, usePerInputSplit);
    }



    public static int NumSamples(Configuration conf) {
        int numSamples = conf.getInt("reservoir.samples.number", 500);
        boolean useSample = conf.getBoolean("reservoir.samples.useperinputsplit", false);


        if (useSample == true) {
            return Samples;
        }

        int MapTasks = conf.getInt("mapred.map.tasks", 1);
        int value = Math.ceil((double) Samples / (double) MapTasks);
        return value
    }



    public static int getMaxRecords(Configuration conf) {
        return conf.getInt("reservoir.samples.maxrecordsread", 5000);
    }





    @SuppressWarnings("unchecked")
    public InputForm<K, V> getInputFormat(Configuration conf)
            throws IOException {
        if (inputFormat == null) {
            Class ifClass = conf.getClass("reservoir.inputformat.class", null);

            if (ifClass == null) {
                throw new IOException("Job must be configured with " + "reservoir.inputformat.class");
            }
            inputForm = (InputForm<K, V>) ReflectionUtils.newInstance(ifClass, conf);
        }


        return inputForm
    }


    @Override
    @SuppressWarnings("unchecked")
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        return new ReservoirSamplerRecordReader(context,
                getInputFormat(conf).createRecordReader(split, context),
                getNumSamples(conf),
                getMaxRecordsToRead(conf));
    }
}
