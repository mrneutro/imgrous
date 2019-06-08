package it.unisa.di.soa2019.similarity;

import it.unisa.di.soa2019.indexing.StringWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SimilarityReducer extends Reducer<StringWritable, DoubleWritable, StringWritable, DoubleWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SimilarityMapper.class);


    @Override
    protected void reduce(StringWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable v : values) {
            sum += v.get();
        }
        context.write(key, new DoubleWritable(sum));
    }
}