package it.unisa.di.soa2019.partitioning;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GroupPartitioningReducer extends Reducer<Text, MapWritable, Text, Text> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(GroupPartitioningMapper.class);

    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws
            IOException, InterruptedException {
//        log.info("" + key + " values " + values.toString() + " on " + this.hashCode());
        HashMap<String, Double> mymap = new HashMap<String, Double>();
        for (MapWritable map : values) {
            Set<Writable> list = map.keySet();

            for (Writable iterKey : list) {
                String iterKeyStr = iterKey.toString();
                if (mymap.containsKey(iterKeyStr)) {
                    mymap.put(iterKeyStr, mymap.get(iterKeyStr) + ((IntWritable) map.get(iterKey)).get());
                } else {
                    mymap.put(iterKeyStr, ((DoubleWritable) map.get(iterKey)).get());
                }
            }
        }

        double norma = 0;
        for (Map.Entry<String, Double> entry : mymap.entrySet()) { //calcolo della norma
            norma += Math.pow(entry.getValue(), 2);
        }
        norma = Math.sqrt(norma);
        StringBuilder outString = new StringBuilder();
        for (Map.Entry<String, Double> entry : mymap.entrySet()) {
            entry.setValue(entry.getValue()/norma);
            outString.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }
        log.info("key: " + key + ", value: " + outString);
        context.write(key, new Text(outString.toString()));
    }
}