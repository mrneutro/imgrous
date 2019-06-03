package it.unisa.di.soa2019;

import it.unisa.di.soa2019.indexing.IndexingMapper;
import it.unisa.di.soa2019.indexing.IndexingReducer;
import it.unisa.di.soa2019.indexing.StringWritable;
import it.unisa.di.soa2019.partitioning.GroupPartitioningMapper;
import it.unisa.di.soa2019.partitioning.GroupPartitioningReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DsDriver {
    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        String input = args[0];

        Job j1 = Job.getInstance(conf, "DocSim - Group partitioning");

        j1.setJarByClass(DsDriver.class);
        j1.setMapperClass(GroupPartitioningMapper.class);
        j1.setReducerClass(GroupPartitioningReducer.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(MapWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        Date date = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyymmdd_hhmmss");
        String strDate = dateFormat.format(date);
        FileOutputFormat.setOutputPath(j1, new Path(args[1] + strDate, "inter"));


        FileInputFormat.addInputPath(j1, new Path(input));
        boolean err = j1.waitForCompletion(true);
        if (err) {
//            System.exit(-1);
        }

        conf = new Configuration();
        Job j2 = Job.getInstance(conf, "DocSim - Indexing");

        j2.setJarByClass(DsDriver.class);
        j2.setMapperClass(IndexingMapper.class);
        j2.setReducerClass(IndexingReducer.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(StringWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(j2, new Path(args[1] + strDate, "inter/part-r-00000"));
        FileOutputFormat.setOutputPath(j2, new Path(args[1] + strDate, "final"));

        err = j2.waitForCompletion(true);
        System.out.println(err);
       /* j1 = Job.getInstance(conf, "Final");
//        j1.setNumReduceTasks(tasks);
        j1.setJarByClass(DocumentSimilarity.class);
        j1.setMapperClass(FinalMapper.class);
        j1.setReducerClass(FinalReducer.class);
//        j1.setPartitionerClass(FinalPartitioner.class);
        j1.setOutputKeyClass(IntPair.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1, temp);
        FileOutputFormat.setOutputPath(j1, new Path(output));
        j1.waitForCompletion(true);*/

    }

}
