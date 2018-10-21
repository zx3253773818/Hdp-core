package cn.tl.demo1.driver;

import java.util.StringTokenizer;

public class Test {

	public static void main(String[] args) {
		String s = "12132454f jfhg";
		String[] strArr = s.split("");
		for (String string : strArr) {
			System.out.println(string);
		}
		System.out.println("===========================");
		StringTokenizer strt = new StringTokenizer(s);
		while (strt.hasMoreElements()) {
			System.out.println(strt.nextToken());
		}
	}

}
