package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.logging.Logger;

public class WcMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());
    private MapWritable map;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        map = new <Text, IntWritable>MapWritable();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        context.write(new Text("" + map.hashCode()), map);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        String line = value.toString();
//        String[] words = line.replaceAll("[^a-zA-Z0-9 ]", "").split(" ");
        String[] words = line.split(" ");

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


    }
}