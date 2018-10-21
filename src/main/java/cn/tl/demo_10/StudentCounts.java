package cn.tl.demo_10;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zhangxin 9、列筛选不去重) 在 hdfs 目录/tmp/table/student 中存在 student.txt 文件，按
 *         tab 分隔，字段名为(学号，姓名，课程号，班级名称），选择学号和班级名称列不去重输出，输出 结果结构按
 *         tab分隔后的两个字段为（学号，班级名称）。
 */
public class StudentCounts {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set(name, value);
		Job job = Job.getInstance(conf, "counts");
		job.setJarByClass(StudentCounts.class);
		job.setMapperClass(MyMapper.class);
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
			nv.set(strArr[3]);
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