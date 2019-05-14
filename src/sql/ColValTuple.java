package sql;

import java.util.ArrayList;

public class ColValTuple {
	public String tableName = null;
	public String columnName = null;
	public Value value = null;
	
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
	
	// Deep Copy
	private ColValTuple(ColValTuple cv) {
		tableName = (cv.tableName == null)? null : new String(cv.tableName);
		columnName = (cv.columnName == null)? null : new String(cv.columnName);
		value = (cv.value == null)? null : new Value(cv.value);
	}
	
	// Reconstruct ColValTuple List in order of colList
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
