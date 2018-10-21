package cn.tl.demo8;

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
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author zhangxin 在 hdfs 目录/tmp/table/student 中存在 student.txt 文件，hdfs 目录
 *         /tmp/table/student_location 中存在 student_location.txt 文件，求两个 hdfs
 *         目录中共有多少行？
 */
public class TwoFileLineCounts {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		GenericOptionsParser op = new GenericOptionsParser(conf, args);
		Job job = Job.getInstance(conf, "counts");
		job.setJarByClass(TwoFileLineCounts.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);// reducer输入输出一致时可写
		job.setReducerClass(MyReducer.class);
		// job.setMapOutputKeyClass(Text.class);//mapper和reducer输出不一致时要写
		// job.setMapOutputValueClass(IntWritable.class);//mapper和reducer输出不一致时要写
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPaths(job, new Path(args[0]) + ","
				+ new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text nk = new Text("总共的行数：");
	private IntWritable one = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Context context)
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
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : arg1) {
			sum += val.get();
		}
		nv.set(sum);
		arg2.write(arg0, nv);
	}
}