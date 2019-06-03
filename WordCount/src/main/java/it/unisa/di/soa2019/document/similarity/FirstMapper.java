package it.unisa.di.soa2019.document.similarity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.logging.Logger;

public class FirstMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(FirstMapper.class);
    private MapWritable map;
    private String[] values;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try{
            log.info("FirstMapper key: " + values[2] + "map: " + map.toString());
            context.write(new Text(values[2]), map);
        } catch (NullPointerException e) {
            log.info("Void or malformed message.to_id.channel_id");
        } catch (Exception e){
            log.error("Exception in FirstMapper.cleanup: "+e);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        map = new <Text, IntWritable>MapWritable();
    }

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            values = line.split(",", 4);
//                log.info("values.length: " + values.length);
            String[] words = values[3].split(" ");

            for (String word : words) {
                Text writableWord = new Text(word);
                if (map.containsKey(writableWord)) {
                    IntWritable prev = (IntWritable) map.get(writableWord);
                    prev.set(prev.get() + 1);
                    map.put(writableWord, prev);
                } else {
                    map.put(writableWord, new IntWritable(1));
                }
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }
}
