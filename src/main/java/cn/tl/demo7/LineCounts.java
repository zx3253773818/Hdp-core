package cn.tl.demo7;

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
 * @author zhangxin 在 hdfs 目录/tmp/tianliangedu/input/wordcount
 *         目录中有一系列文件，求这些文件一共有多少行？（类似于 mysql 数据库中的 select count(*) from table)
 */
public class LineCounts {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "counts");
		job.setJarByClass(LineCounts.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);// reducer输入输出一致时可写
		job.setReducerClass(MyReducer.class);
		// job.setMapOutputKeyClass(Text.class);//mapper和reducer输出不一致时要写
		// job.setMapOutputValueClass(IntWritable.class);//mapper和reducer输出不一致时要写
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text nk = new Text("总共的行数：");
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] strArr = value.toString().split("\\n");
		for (String str : strArr) {
			nk.set(str);
			context.write(nk, one);
		}
	}
}

class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable nv = new IntWritable();

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : arg1) {
			sum += val.get();
		}
		nv.set(sum);
		arg2.write(arg0, nv);
	}
}