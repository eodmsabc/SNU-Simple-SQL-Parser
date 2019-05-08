package sql;

import java.util.ArrayList;

//TableElement class for parsing
public class Parse_Create {
	public Parse_Create(DataType dt) {
		type = ETYPE.COLDEF;
		dataType = dt;
	}

	public Parse_Create(DataType dt, int chlen) {
		type = ETYPE.COLDEF;
		dataType = dt;
		charlen = chlen;
	}

	public Parse_Create(ArrayList<String> pr) {
		type = ETYPE.PRIMARY;
		primary = pr;
	}

	public Parse_Create(ArrayList<String> fo, String ref, ArrayList<String> rKeys) {
		type = ETYPE.FOREIGN;
		foreign = new ForeignKeyConstraint(fo, ref, rKeys);
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
	public ForeignKeyConstraint foreign;
}