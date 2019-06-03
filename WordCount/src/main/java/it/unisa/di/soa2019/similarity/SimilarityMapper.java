package it.unisa.di.soa2019.similarity;

import it.unisa.di.soa2019.indexing.StringWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SimilarityMapper extends Mapper<LongWritable, MapWritable, StringWritable, IntWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SimilarityMapper.class);
    private Map<String, MapWritable> groupMap;
    private String[] values;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
//        map = new <Text, IntWritable>MapWritable();
        groupMap = new HashMap<>(500);
    }

    @Override
    protected void map(LongWritable term, MapWritable value, Context context) throws IOException, InterruptedException {
        Text[] keys = (Text[]) value.keySet().toArray();
        for (int i = 0; i < keys.length; i++) {
            for (int j = i + 1; j < keys.length; j++) {
                Writable groupIID = keys[i];
                IntWritable frequencyI = (IntWritable) value.get(keys[i]);

                Writable groupJID = keys[j];
                IntWritable frequencyJ = (IntWritable) value.get(keys[j]);

                IntWritable w = new IntWritable(frequencyI.get() * frequencyJ.get());
                StringWritable sp = new StringWritable(groupIID.toString(), groupJID.toString());
                context.write(sp, w);
            }
        }
    }
}
