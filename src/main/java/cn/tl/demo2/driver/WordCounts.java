package cn.tl.demo2.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.tl.demo2.mapper.MyTokenizerMapper;
import cn.tl.demo2.reducer.IntSumReducer;

public class WordCounts {

	public static void main(String[] args) throws Exception {
		Configuration hdpConf = new Configuration();
		Job job = Job.getInstance(hdpConf, "wordCounts");
		job.setJarByClass(WordCounts.class);
		job.setMapperClass(MyTokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
