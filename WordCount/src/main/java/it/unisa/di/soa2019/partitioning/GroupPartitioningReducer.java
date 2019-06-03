package it.unisa.di.soa2019.partitioning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GroupPartitioningReducer extends Reducer<Text, MapWritable, Text, Text> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(GroupPartitioningMapper.class);
    Text key;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
//        megamap = new <Text, IntWritable>MapWritable();

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

    }

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws
            IOException, InterruptedException {
//        log.info("" + key + " values " + values.toString() + " on " + this.hashCode());
        HashMap<String, Integer> mymap = new HashMap<String, Integer>();
        for (MapWritable map : values) {
            Set<Writable> list = map.keySet();

            for (Writable iterKey : list) {
                String iterKeyStr = iterKey.toString();
                if (mymap.containsKey(iterKeyStr)) {
                    mymap.put(iterKeyStr, mymap.get(iterKeyStr) + ((IntWritable) map.get(iterKey)).get());
                } else {
                    mymap.put(iterKeyStr, ((IntWritable) map.get(iterKey)).get());
                }
            }
        }

        StringBuilder outString = new StringBuilder();
        for (Map.Entry<String, Integer> entry : mymap.entrySet()) {
            outString.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }
        log.info("key: " + key + ", value: " + outString);
        context.write(key, new Text(outString.toString()));
    }
}