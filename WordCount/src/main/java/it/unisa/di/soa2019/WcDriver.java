package it.unisa.di.soa2019;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WcDriver extends Configured implements Tool {

    public static void secondary_main(String[] args) throws Exception {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
        int exitCode = ToolRunner.run(new WcDriver(), args);
        System.out.println("Exit code :" + exitCode);
        System.exit(exitCode);
    }

    public int run(String[] arg0) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        job1.setJobName("LowOrbitWordCount");
        job1.setJarByClass(WcDriver.class);
        job1.setMapperClass(WcMapper.class);
        job1.setCombinerClass(WcCombiner.class);
        job1.setNumReduceTasks(50);
        job1.setReducerClass(WcReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(MapWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job1, new Path(arg0[0]));

        Date date = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyymmdd_hhmmss");
        String strDate = dateFormat.format(date);
        FileOutputFormat.setOutputPath(job1, new Path(arg0[1] + strDate, "inter"));


        int ecode = job1.waitForCompletion(true) ? 0 : 1;

//        if (ecode != 0) {
//            return ecode;
//        }
//
//        Job job2 = Job.getInstance(conf);
//        job2.setJobName("Frequency analyzer");
//
//        job2.setJarByClass(WcDriver.class);
//        job2.setMapperClass(SwapperMapper.class);
//        job2.setReducerClass(SumReducer.class);
//        job2.setNumReduceTasks(1);
//        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
//        job2.setOutputKeyClass(LongWritable.class);
//        job2.setOutputValueClass(Text.class);
//        job2.setInputFormatClass(SequenceFileInputFormat.class);
//        FileInputFormat.addInputPath(job2, new Path(arg0[1] + strDate, "inter/part-r-00000"));
//        FileOutputFormat.setOutputPath(job2, new Path(arg0[1] + strDate, "final"));
//        ecode = job2.waitForCompletion(true) ? 0 : 1;
        return ecode;
    }

}