package it.unisa.di.soa2019.indexing;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexingMapper extends Mapper<LongWritable, Text, Text, StringWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(IndexingMapper.class);
    private Map<String, MapWritable> groupMap;
    private String[] values;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
//        map = new <Text, IntWritable>MapWritable();
        groupMap = new HashMap<>(500);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();

            String[] pieces = line.split("\t");
            String groupId = pieces[0];
            String[] termList = pieces[1].split(",");
            for (String termPair : termList) {
                String[] termPieces = termPair.split(":");
                String k = termPieces[0];
                String v = termPieces[1];
                context.write(new Text(k), new StringWritable(groupId, v));
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }
}
