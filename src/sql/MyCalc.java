package sql;

public class MyCalc {
	// true:	1
	// unknown:	0
	// false:  -1
	public static int and(int a, int b) {
		return a > b ? b : a;
	}
	public static int or(int a, int b) {
		return a > b ? a : b;
	}
	public static int not(int a) {
		return -a;
	}
	public static int max(int a, int b) {
		return a > b ? a : b;
	}
}
