package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

public class WcCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());

    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        log.info("key=" + key + "");

        int sum = 0;
        for (IntWritable v : value)
            sum += v.get();

        context.write(key, new IntWritable(sum));
    }
}
