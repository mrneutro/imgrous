package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

public class WcCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());

    public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
        log.info("key=" + key + "" +" on "+this.hashCode());

        int sum = 0;
        for (LongWritable v : value)
            sum += v.get();

        context.write(key, new LongWritable(sum));
    }
}
