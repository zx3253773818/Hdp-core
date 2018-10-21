package cn.tl.demo__3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author zhangxin 3、给定如下图的学生成绩表，表结构为（学号，姓名，课程名称，成绩）,字段间分隔符为 tab，其 hdfs
 *         路径为/tmp/
 *         table/student_score.txt，请使用传统的二次排序方法，实现按学号第一顺序，相同学号情况下按成绩倒序排列，
 *         输出到个人用户的家目录下，输出结果字段为（学号，姓名，课程名称，成绩），按 tab 分隔即可。
 */
public class TwoSortStudent {

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
		job.setJarByClass(TwoSortStudent.class);
		job.setMapperClass(MyMapper.class);
		// job.setPartitionerClass(MyPartitoner.class);// 指定分区规则
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
	private List<StudentInfo> list = new ArrayList<StudentInfo>();
	private Text nk = new Text();

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		for (Text st : arg1) {
			String[] s = st.toString().split("\\t");
			list.add(new StudentInfo(s[0], s[1], Integer.parseInt(s[2])));
			Collections.sort(list, new Comparator<StudentInfo>() {

				@Override
				public int compare(StudentInfo o1, StudentInfo o2) {
					return o1.getScore() - o2.getScore();
				}
			});
		}
		for (StudentInfo stu : list) {
			nk.set(stu.getName() + "\t" + stu.getClassName() + "\t"
					+ String.valueOf(stu.getScore()));
			arg2.write(arg0, nk);
			// break; //简单去重
		}

	}
}

class StudentInfo {
	private String name;
	private String className;
	private Integer score;

	public StudentInfo(String name, String className, Integer score) {
		super();
		this.name = name;
		this.className = className;
		this.score = score;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public Integer getScore() {
		return score;
	}

	public void setScore(Integer score) {
		this.score = score;
	}

}
