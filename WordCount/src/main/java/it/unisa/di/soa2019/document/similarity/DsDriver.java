package it.unisa.di.soa2019.document.similarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DsDriver {
    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];
//        String tmpTask = args[2];
//        int tasks = Integer.parseInt(tmpTask);
        Job job = Job.getInstance(conf, "First");
//        job.setNumReduceTasks(tasks);
        job.setJarByClass(DsDriver.class);
        job.setMapperClass(FirstMapper.class);
        job.setCombinerClass(FirstCombiner.class);
        job.setReducerClass(FirstReducer.class);
//        job.setPartitionerClass(FirstPartitioner.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path temp = new Path(output + "_temp");
        FileOutputFormat.setOutputPath(job, temp);
        job.waitForCompletion(true);

       /* job = Job.getInstance(conf, "Final");
//        job.setNumReduceTasks(tasks);
        job.setJarByClass(DocumentSimilarity.class);
        job.setMapperClass(FinalMapper.class);
        job.setReducerClass(FinalReducer.class);
//        job.setPartitionerClass(FinalPartitioner.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, temp);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);*/


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
