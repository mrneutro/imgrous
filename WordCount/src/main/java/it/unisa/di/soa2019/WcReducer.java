package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

public class WcReducer extends Reducer<Text, MapWritable, Writable, IntWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());
    private MapWritable megamap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        megamap = new <Text, IntWritable>MapWritable();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        Set<Writable> list = megamap.keySet();
        for (Writable iterKey : list) {
            IntWritable iterVal = (IntWritable) megamap.get(iterKey);
            context.write(iterKey, iterVal);
        }
    }

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws
            IOException, InterruptedException {
        log.info("" + key + " values " + values.toString() + " on " + this.hashCode());

        for (MapWritable map : values) {
            Set<Writable> list = map.keySet();
            for (Writable iterKey : list) {
                IntWritable iterVal = (IntWritable) map.get(iterKey);
                if (megamap.containsKey(iterKey)) {
                    int localSum = ((IntWritable) megamap.get(iterKey)).get() + iterVal.get();
                    megamap.put(iterKey, new IntWritable(localSum));
                } else {
                    megamap.put(iterKey, iterVal);
                }
            }
        }
    }
}