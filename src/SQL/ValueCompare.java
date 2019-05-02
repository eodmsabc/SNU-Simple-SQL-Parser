package SQL;

import java.util.ArrayList;

public class ValueCompare {
	public String tableName = null;
	public String columnName = null;
	public Value value = null;
	public Comparator comparator = null;
	
	public ValueCompare(String table, String column, Value val, Comparator comp) {
		tableName = table;
		columnName = column;
		value = val;
		comparator = comp;
	}
	
	/*
	public ValueCompare(String colName, Value val) {
		columnName = colName;
		value = val;
	}
	
	public ValueCompare(Value val) {
		value = val;
	}
	*/
	
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
		}
		
		return ret;
	}
	
	public static ArrayList<ValueCompare> vcColumnFilter(ArrayList<ValueCompare> vcList, ArrayList<String> colList) {
		ArrayList<ValueCompare> filtered = new ArrayList<ValueCompare>();
		for (ValueCompare vc : vcList) {
			if (colList.contains(vc.columnName)) {
				filtered.add(vc);
			}
		}
		return filtered;
	}
}
