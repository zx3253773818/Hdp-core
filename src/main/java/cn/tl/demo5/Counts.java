package cn.tl.demo5;

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

/**
 * @author zhangxin
 *在 hdfs 目录/tmp/tianliangedu/input/wordcount 目录中有一系列文件，
 *内容为","号分隔，分隔后的元素
 *均为数值类型、字母、中文，求数值类型、字母类型、中文类型各自的次数
 */
public class Counts {

	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "counts");
		job.setJarByClass(Counts.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private Text newKey1 = new Text("数字个数：");
		private Text newKey2 = new Text("字母个数：");
		private Text newKey3 = new Text("中文个数：");
		private IntWritable one = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] s = value.toString().split(",");
			for (String str : s) {
				if (key.toString().matches("\\d+")) {
					newKey1.set(str);
					context.write(newKey1, one);
				} else if (key.toString().matches("^[a-zA-Z],{0,}$")) {
					newKey2.set(str);
					context.write(newKey2, one);
				} else if (key.toString().matches("^[u4e00-u9fa5],{0,}$")) {
					newKey3.set(str);
					context.write(newKey3, one);
				}
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable nv = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable in : value) {
				sum += in.get();
			}
			nv.set(sum);
			context.write(key, nv);
		}
	}
}
