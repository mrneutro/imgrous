package it.unisa.di.soa2019.partitioning;

import it.unisa.di.soa2019.StopWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.LoggerFactory;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;
import org.tartarus.snowball.ext.italianStemmer;
import org.tartarus.snowball.ext.russianStemmer;
import org.tartarus.snowball.ext.spanishStemmer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GroupPartitioningMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(GroupPartitioningMapper.class);
    private Map<String, MapWritable> groupMap;
    private String[] values;


    private HashMap<String, SnowballStemmer> stemmersMap;
    private SnowballStemmer genericStemmer = (SnowballStemmer) new englishStemmer();


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
        stemmersMap = new HashMap<String, SnowballStemmer>(4);
        stemmersMap.put("it", new italianStemmer());
        stemmersMap.put("ru", new russianStemmer());
        stemmersMap.put("es", new spanishStemmer());
        stemmersMap.put("en", new englishStemmer());
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
//            log.info(line);
            values = line.split(",");
            String lang = null;
            if (values.length > 5) {
                lang = values[values.length - 1];
                for (int i = 4; i < values.length - 1; i++) {
                    values[3] = values[3] + values[i]; // TODO use string builder here
                }
            } else {
                lang = values[4];
            }

//                log.info("values.length: " + values.length);
            String[] words = values[3].replaceAll("\\n", " ").replaceAll("[^\b a-zA-Z0-9'а-яА-Я]", "").toLowerCase().split(" ");
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
                SnowballStemmer currentStemmer = null;
                if (stemmersMap.containsKey(lang)) {
                    currentStemmer = stemmersMap.get(lang);
                } else {
                    currentStemmer = genericStemmer;
                }
                if (StopWords.getInstance().isStopWord(word)) {
                    continue;
                }
                currentStemmer.setCurrent(word);
                currentStemmer.stem();
                String stemmed = currentStemmer.getCurrent();

                Text writableWord = new Text(stemmed);
                if (localMap.containsKey(writableWord)) {
                    IntWritable prev = (IntWritable) localMap.get(writableWord);
                    prev.set(prev.get() + 1);
                    localMap.put(writableWord, prev);
                } else {
                    localMap.put(writableWord, new IntWritable(1));
                }

//                Text writableWord = new Text(word);
//                if (localMap.containsKey(writableWord)) {
//                    IntWritable prev = (IntWritable) localMap.get(writableWord);
//                    prev.set(prev.get() + 1);
//                    localMap.put(writableWord, prev);
//                } else {
//                    localMap.put(writableWord, new IntWritable(1));
//                }
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }
}
