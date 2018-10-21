package cn.tl.demo6;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.tl.demo5.Counts;
import cn.tl.demo5.Counts.MyMapper;
import cn.tl.demo5.Counts.MyReducer;

/**
 * @author zhangxin 在 hdfs 目录/tmp/tianliangedu/input/wordcount 目录中有一系列文件
 *         ，内容为","号分隔，同时在 hdfs 路径/tmp/tianliangedu/black.txt 黑名单文件，
 *         一行一个单词用于存放不记入统计的单词列表。求按","号分隔的各个元素去除掉黑名 单后的出现频率，输出到目录
 *         /tmp/tianliangedu/output/个人用户名的 hdfs 目录中。
 */
public class noBlackCounts {

	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "counts");
		job.setJarByClass(Counts.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReduce.class);// reducer输入输出一致时可写
		job.setReducerClass(MyReducer.class);
		// job.setMapOutputKeyClass(Text.class);//mapper和reducer输出不一致时要写
		// job.setMapOutputValueClass(IntWritable.class);//mapper和reducer输出不一致时要写
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Mymapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private Text nk = new Text();
		private final IntWritable one = new IntWritable(1);
		private Set<String> set = new HashSet<String>();

		private void initBlack(Context context, Set<String> set)
				throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsis = fs.open(new Path(
					"/user/zhangxin/black.txt"));
			BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
			String line = null;
			while ((line = br.readLine()) != null) {
				set.add(line);
			}
		}

		@Override
		protected void setup(
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			initBlack(context, set);
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] s = value.toString().split(",");
			for (String str : s) {
				if (!set.contains(str)) {
					nk.set(str);
					context.write(nk, one);
				}
			}
		}
	}

	public static class MyReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable in : values) {
				sum += in.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
