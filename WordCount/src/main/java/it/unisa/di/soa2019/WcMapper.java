package it.unisa.di.soa2019;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class WcMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Logger log = Logger.getLogger(this.getClass().getName());

    @Override
    public void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.replaceAll("[^a-zA-Z0-9 ]", "").split(" ");
        Map<String, Integer> map = new HashMap<>(words.length);
        for (String word : words) {
            if (map.containsKey(word)) {
                map.put(word, map.get(word) + 1);
            } else {
                map.put(word, 1);
            }
        }
        log.info("SPACE REDUCTION " + (words.length - map.size()) +" on "+this.hashCode());
        Text keyText = new Text();
        LongWritable valueInt = new LongWritable();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            keyText.set(entry.getKey());
            valueInt.set(entry.getValue());
            context.write(keyText, valueInt);
        }
    }
}