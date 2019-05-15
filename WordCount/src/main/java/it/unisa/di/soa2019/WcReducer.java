package it.unisa.di.soa2019;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

public class WcReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws
            IOException, InterruptedException {
        log.info("" + key + " values " + values.toString() + " on " + this.hashCode());
        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        context.write(key, new LongWritable(sum));
    }
}