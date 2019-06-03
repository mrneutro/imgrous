package it.unisa.di.soa2019.partitioning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GroupPartitioningMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(GroupPartitioningMapper.class);
    private Map<String, MapWritable> groupMap;
    private String[] values;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try {
//            log.info("IndexingMapper key: " + values[2] + "map: " + map.toString());
            for (Map.Entry<String, MapWritable> pair : groupMap.entrySet()) {
                if (!pair.getValue().isEmpty()) {
                    context.write(new Text(pair.getKey()), pair.getValue());
                }
            }
        } catch (NullPointerException e) {
            log.info("Void or malformed message.to_id.channel_id");
        } catch (Exception e) {
            log.error("Exception in IndexingMapper.cleanup: " + e);
        }
    }

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
            log.info(line);
            values = line.split(",", 5);
//                log.info("values.length: " + values.length);
            String[] words = values[3].replaceAll("[^\b a-zA-Z0-9]", "").toLowerCase().split(" ");
            String group_id = values[2];
            MapWritable localMap = null;
            if (groupMap.containsKey(group_id)) {
                localMap = groupMap.get(group_id);
            } else {
                localMap = new <Text, IntWritable>MapWritable();
                groupMap.put(group_id, localMap);
            }
            for (String word : words) {
                if (word.length() == 0) {
                    continue;
                }
                Text writableWord = new Text(word);
                if (localMap.containsKey(writableWord)) {
                    IntWritable prev = (IntWritable) localMap.get(writableWord);
                    prev.set(prev.get() + 1);
                    localMap.put(writableWord, prev);
                } else {
                    localMap.put(writableWord, new IntWritable(1));
                }
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }
}
