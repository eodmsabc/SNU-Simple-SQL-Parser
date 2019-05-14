package sql;

import java.util.ArrayList;

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
	
	public ColValTuple(String col, Value v) {
		columnName = col;
		value = v;
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
	
	// Deep Copy
	private ColValTuple(ColValTuple cv) {
		tableName = (cv.tableName == null)? null : new String(cv.tableName);
		columnName = (cv.columnName == null)? null : new String(cv.columnName);
		value = (cv.value == null)? null : new Value(cv.value);
		comparator = (cv.comparator == null)? null : cv.comparator;
	}
	
	static ArrayList<ColValTuple> columnFilter(ArrayList<String> colList, ArrayList<ColValTuple> cvList) {
		ArrayList<ColValTuple> retList = new ArrayList<ColValTuple>();
		for (String col : colList) {
			for (ColValTuple cv : cvList) {
				if (col.equals(cv.columnName)) {
					retList.add(new ColValTuple(cv));
				}
			}
		}
		return retList;
	}
}
