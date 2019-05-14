package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.logging.Logger;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
            IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}