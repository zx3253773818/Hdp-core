package cn.tl.demo3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntSumCounts {

	public static void main(String[] args) throws Exception {
		Configuration hdpConf = new Configuration();
		Job job = Job.getInstance(hdpConf, "intSumCounts");
		job.setJarByClass(IntSumCounts.class);
		job.setMapperClass(MyTokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyTokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private Text newkey = new Text("所有数值和为");
		private IntWritable in = new IntWritable();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] s = value.toString().split(",");
			for (String string : s) {
				if (string.matches("\\d+")) {
					in.set(Integer.parseInt(string));
					context.write(newkey, in);
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable value = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable intWritable : values) {
				sum += intWritable.get();
			}
			value.set(sum);
			context.write(key, value);
		}
	}
}
