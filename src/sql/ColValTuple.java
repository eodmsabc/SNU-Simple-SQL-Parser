package sql;

public class ColValTuple {
	public String tableName = null;
	public String columnName = null;
	public Value value = null;
	public Comparator comparator = null;
	
	public ColValTuple(String table, String column, Value val, Comparator comp) {
		tableName = table;
		columnName = column;
		value = val;
		comparator = comp;
	}
	
	public ColValTuple(Value val) {
		value = val;
	}
	
	public ColValTuple(String t, String c) {
		tableName = t;
		columnName = c;
	}
	
	public ColValTuple(String col) {
		columnName = col;
	}
	
	public boolean isConstValue() {
		return (value != null);
	}
	
	public boolean check(Value base) {
		int diff = value.compareTo(base);
		boolean ret = false;
		
		switch(comparator) {
			case EQ:
				if (diff == 0) ret = true;
				break;
			case NEQ:
				if (diff != 0) ret = true;
				break;
				
			case GTE:
				if (diff == 0) ret = true;
			case GT:
				if (diff > 0) ret = true;
				break;
				
			case LTE:
				if (diff == 0) ret = true;
			case LT:
				if (diff < 0) ret = true;
				break;
				
			default:
				return ret;
		}
		
		return ret;
	}
}
