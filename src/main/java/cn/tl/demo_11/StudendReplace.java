package cn.tl.demo_11;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author zhangxin 11、 (列值 replace 操作)在 hdfs 目录/tmp/table/student 中存在
 *         student.txt 文件，按
 *         tab分隔，字段名为(学号，姓名，课程号，班级名称），将班级名称为"计算机*班"的更换成"计算机科学与技术*班",不做去重，输出结果结构按
 *         tab 分隔后的两个字段为（学号，班级名称）。
 */
public class StudendReplace {

	public static void main(String[] args) {

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
			if (strArr[3] == "计算机*班") {
				nv.set("计算机科学与技术*班");
			} else {
				nv.set(strArr[3]);
			}
			context.write(nk, nv);
		} else {
			try {
				throw new Exception("数据不规则");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

class MyReducer extends Reducer<Text, Text, Text, Text> {
	StringBuffer sb = new StringBuffer();

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2)
			throws IOException, InterruptedException {
		for (Text st : arg1) {
			arg2.write(arg0, st);
			// break; //简单去重
		}
	}
}