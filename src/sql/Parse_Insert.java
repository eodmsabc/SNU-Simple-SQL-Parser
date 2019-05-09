package sql;

import java.util.ArrayList;

public class Parse_Insert {
	public ArrayList<String> colList;
	public ArrayList<Value> valList;
	
	public Parse_Insert(ArrayList<String> colList, ArrayList<Value> valList) {
		this.colList = colList;
		this.valList = valList;
	}
}
