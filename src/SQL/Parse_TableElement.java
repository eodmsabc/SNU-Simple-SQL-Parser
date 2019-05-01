package SQL;

import java.util.ArrayList;

//TableElement class for parsing
public class Parse_TableElement {
	public Parse_TableElement(DataType dt) {
		type = ETYPE.COLDEF;
		dataType = dt;
	}

	public Parse_TableElement(DataType dt, int chlen) {
		type = ETYPE.COLDEF;
		dataType = dt;
		charlen = chlen;
	}

	public Parse_TableElement(ArrayList<String> pr) {
		type = ETYPE.PRIMARY;
		primary = pr;
	}

	public Parse_TableElement(ArrayList<String> fo, String ref, ArrayList<String> rKey) {
		type = ETYPE.FOREIGN;
		foreign = fo;
		refTable = ref;
		refKey = rKey;
	}

	public enum ETYPE {
		COLDEF, PRIMARY, FOREIGN
	};

	public ETYPE type;
	public String columnName;
	public DataType dataType;
	public int charlen;
	public boolean nullable;
	public ArrayList<String> primary;
	public ArrayList<String> foreign;
	public String refTable;
	public ArrayList<String> refKey;
}