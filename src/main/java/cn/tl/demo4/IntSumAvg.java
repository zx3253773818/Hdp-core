package cn.tl.demo4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntSumAvg {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "intSumAvg");
		job.setJarByClass(IntSumAvg.class);
		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyMapper extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		private IntWritable size = new IntWritable(1);
		private IntWritable result = new IntWritable();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] strArr = value.toString().split(",");
			for (String str : strArr) {
				if (str.matches("\\d+")) {
					int i = Integer.parseInt(str);
					result.set(i);
					context.write(size, result);
				}
			}
		}
	}

	public static class MyReducer extends
			Reducer<IntWritable, IntWritable, Text, IntWritable> {
		private Text newKey = new Text("所有数值和的均值为：");
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int flag = 0;
			for (IntWritable val : values) {
				sum += val.get();
				flag++;
			}
			result.set(sum / flag);
			context.write(newKey, result);
		}
	}
}
