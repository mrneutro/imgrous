package it.unisa.di.soa2019.document.similarity;

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

public class FirstReducer extends Reducer<Text, MapWritable, Text, Text> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(FirstMapper.class);

    private HashMap<String, Integer> mymap;
    Text key;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
//        megamap = new <Text, IntWritable>MapWritable();
        mymap = new HashMap<String, Integer>();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        String outString = "";
        for (Map.Entry<String, Integer> entry : mymap.entrySet()) {
            outString = outString + entry.getKey() + ":" + entry.getValue() + ",";
        }
        log.info("key: "+ key + ", value: " + outString);
        context.write(key, new Text(outString));
    }

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws
            IOException, InterruptedException {
//        log.info("" + key + " values " + values.toString() + " on " + this.hashCode());
        this.key = key;

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
    }
}