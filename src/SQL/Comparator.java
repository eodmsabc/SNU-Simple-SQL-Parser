package SQL;

public enum Comparator {
	EQ, NEQ, GT, GTE, LT, LTE, IN, INN;
	public static Comparator convert(String comp) {
		if (comp.equals("=")) {
			return EQ;
		}
		else if (comp.equals("!=")) {
			return NEQ;
		}
		else if (comp.equals(">")) {
			return GT;
		}
		else if (comp.equals(">=")) {
			return GTE;
		}
		else if (comp.equals("<")) {
			return LT;
		}
		else {
			return LTE;
		}
	}
	public static Comparator convert(boolean b) {
		if (b) return IN;
		else return INN;
	}
}
