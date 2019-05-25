package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class WcReducer extends Reducer<Text, MapWritable, Writable, IntWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());
    //    private MapWritable megamap;
    private HashMap<String, Integer> mymap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
//        megamap = new <Text, IntWritable>MapWritable();
        mymap = new HashMap<String, Integer>();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Text iterKey = new Text();
        IntWritable iterVal = new IntWritable();
        for (Map.Entry<String, Integer> entry : mymap.entrySet()) {
            iterKey.set(entry.getKey());
            iterVal.set(entry.getValue());
            context.write(iterKey, iterVal);
        }
    }

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws
            IOException, InterruptedException {
//        log.info("" + key + " values " + values.toString() + " on " + this.hashCode());

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