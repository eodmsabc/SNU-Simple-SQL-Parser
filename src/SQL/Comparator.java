package SQL;

public enum Comparator {
	EQ, NEQ, GT, GTE, LT, LTE;
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
}
