package cn.tl.demo__2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author zhangxin 2、以 hdfs 路径/tmp/table/student_score.txt
 *         为输入，表结构为（学号，姓名，课程名称，成绩），字段间分隔符为 tab，如下图所示。通过设置 reduce 个数为 2，自定义 hash
 *         partition实现将其中姓名为"张一"的放到同一个 reduce 中，非张一的放到其它的 reduce
 *         中，输出结果字段为（学号，姓名，课程名称，成绩），按 tab 分隔即可。
 */
public class ParttitionStudent {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] remainingArgs = gop.getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err
					.println("Usage: yarn jar jar_path main_class_path -D 参数列表 <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "counts");
		job.setJarByClass(ParttitionStudent.class);
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitoner.class);// 指定分区规则
		job.setCombinerClass(MyReducer.class);// reducer输入输出一致时可写
		job.setReducerClass(MyReducer.class);
		// job.setMapOutputKeyClass(Text.class);//mapper和reducer输出不一致时要写
		// job.setMapOutputValueClass(IntWritable.class);//mapper和reducer输出不一致时要写
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class MyMapper extends Mapper<Object, Text, Text, Text> {
	private Text nk = new Text();
	private Text nv = new Text();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strArr = value.toString().split("\\t");
		if (strArr.length == 4) {
			nk.set(strArr[0]);
			nv.set(strArr[1] + "\t" + strArr[2] + "\t" + strArr[3]);
			context.write(nk, nv);
		} else {
			try {
				throw new Exception("数据格式不规则");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

class MyReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		for (Text st : arg1) {
			arg2.write(arg0, st);
			// break; //简单去重
		}

	}
}

class MyPartitoner<K, V> extends Partitioner<K, V> {

	@Override
	public int getPartition(K arg0, V arg1, int arg2) {
		String[] strArr1 = arg1.toString().split("\\t");
		return (strArr1[0] == "张一" ? 0 : 1) % arg2;
	}
}