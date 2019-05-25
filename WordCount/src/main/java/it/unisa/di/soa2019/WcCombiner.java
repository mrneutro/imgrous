package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

public class WcCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable localMap = new <Text, IntWritable>MapWritable();
        for (MapWritable map : values) {
            Set<Writable> list = map.keySet();
            for (Writable iterKey : list) {
                if (localMap.containsKey(iterKey)) {
                    localMap.put(iterKey, new IntWritable(((IntWritable) localMap.get(iterKey)).get() + ((IntWritable) map.get(iterKey)).get()));
                } else {
                    localMap.put(iterKey, map.get(iterKey));
                }
            }
        }
        context.write(key, localMap);
    }
}
