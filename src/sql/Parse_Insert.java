package sql;

import java.util.ArrayList;

public class Parse_Insert {
	public ArrayList<String> colList;
	public ArrayList<Value> valList;
	
	public Parse_Insert(ArrayList<String> colList, ArrayList<Value> valList) {
		this.colList = colList;
		this.valList = valList;
	}
	
	public Value getValue(String colName) {
		if (colList == null) return null;
		
		int idx = colList.indexOf(colName);
		
		if (idx == -1) return null;
		if (idx < 0 || valList.size() <= idx) return null;
		
		return valList.get(idx);
	}
}