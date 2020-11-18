/** sk7970_sabarnikundu_ques1-
     * This is the Custom Record Reader. In this peice of code I have tried to make two arrays one for keys and another array which stores the values to the corresponding keys. 
     * Code for randomness is there in InitilizeRandom class.
     * I have taken reference from : https://github.com/alexholmes/hadoop-book
     */



package edu.nyu.tandon.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;



public class ResSamplerReader<K extends Writable, V extends Writable>
        extends RecordReader {

    private final int Samples;
    private final int Records;       
    private final Configuration conf;
    private final RecordReader<k,v> rr;
    
    private final ArrayList<K> key;
    private final ArrayList<V> val;

    private int i = 0;


    
    public ResSamplerReader(TaskAttemptContext context,
                                        RecordReader<K, V> rr,
                                        int Samples,
                                        int Records) {
        this.conf = context.getConfiguration();
        this.rr = rr;
        this.Samples = Samples;
        this.Records = Records;

        key = new ArrayList<K>(Samples);
        val = new ArrayList<V>(Samples);
    }

    @Override
    public void initializeRandom(InputSplit split,
                           TaskAttemptContext context)
            throws IOException, InterruptedException {
        rr.initializeRandom(split, context);

        Random rand = new Random();
        for (int ini = 0; ini < Records; ini++) {

            if (!rr.nextKeyValue()) {
                break;
            }

            K key = rr.getCurrentKey();
            V val = rr.getCurrentValue();

            if (key.length() <= Samples-1) {
                key.add(WritableUtils.clone(key, conf));
                val.add(WritableUtils.clone(val, conf));
            } 
            
            else {
                int ele = rand.nextInt(ini);
                if (ele <= Samples-1) {
                    key.set(ele, WritableUtils.clone(key, conf));
                    val.set(ele, WritableUtils.clone(val, conf));
                }
            }
        }
    }

    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {

        return i++ < keys.size();
    }

    @Override
    public K getCurrentKey()
            throws IOException, InterruptedException {

        return key.get(i--);
    }

    @Override
    public Object getCurrentValue()
            throws IOException, InterruptedException {

        return val.get(i--);
    }

    @Override
    public float getProgress()
            throws IOException, InterruptedException {

        return Math.min(idx, key.size()) / key.size();
    }

    @Override
    public void close() throws IOException {
        rr.close();
    }
}

