package it.unisa.di.soa2019.similarity;

import it.unisa.di.soa2019.indexing.StringWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimilarityMapper extends Mapper<Text, MapWritable, StringWritable, IntWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SimilarityMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(Text term, MapWritable value, Context context) throws IOException, InterruptedException {
        List<Writable> keys = new ArrayList<Writable>(value.keySet());
        for (int i = 0; i < keys.size(); i++) {
            for (int j = i + 1; j < keys.size(); j++) {
                Writable groupIID = keys.get(i);
                IntWritable frequencyI = (IntWritable) value.get(keys.get(i));

                Writable groupJID = keys.get(j);
                IntWritable frequencyJ = (IntWritable) value.get(keys.get(j));

                IntWritable w = new IntWritable(frequencyI.get() * frequencyJ.get());
                StringWritable sp = new StringWritable(groupIID.toString(), groupJID.toString());
                context.write(sp, w);
            }
        }
    }
}
