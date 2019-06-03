package it.unisa.di.soa2019.indexing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IndexingReducer extends Reducer<Text, StringWritable, Text, MapWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(IndexingMapper.class);

    @Override
    public void reduce(Text key, Iterable<StringWritable> values, Context context) throws
            IOException, InterruptedException {
//        log.info("" + key + " values " + values.toString() + " on " + this.hashCode());
        MapWritable map = new <Text, IntWritable>MapWritable();
        for (StringWritable sw : values) {
            map.put(new Text(sw.getKey()), new IntWritable(Integer.parseInt(sw.getVal())));
        }

        context.write(key, map);
    }
}