
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class PatternMatch {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) 
        {
            System.err.println("grep <in> <out>");
            System.exit(2);
        }

        conf.set("mapred.textoutputformat.separator", "  --->  ");
        

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "grep finder");
        job.setJarByClass(PatternMatch.class);
        job.setMapperClass(TMapper.class);
        job.setInputFormatClass(ResSamplerInputForm.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        ResSamplerInputForm.setInputFormat(job, TextInputFormat.class);

        
        // Set according to the no. of samples that you want to run
        ResSamplerInputForm.setNumSamples(job, 1);
        ResSamplerInputForm.setMaxRecordsToRead(job, 100);
        ResSamplerInputForm.setUseSamplesNumberPerInputSplit(job, true);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 
     * sample mapper that formats the lines and prints them.
     */

       public static class TMapper 
            extends Mapper<Object, Text, Text, Text> {

        private Text text = new Text();
        private Text display = new Text();

        public void map (Object key, Text val, Mapper.Context context
        ) throws IOException, InterruptedException {

            String Line = val.toString();

                if (len(Line) <= 54) {
                    display.set(Line);    
                }

                else {
                    display.set(Line.substring(0, 40));
                    
                }

               word.set("Lines " + displayLine.toString());
        

        }
    }
}


