package SQL;

import java.io.Serializable;

//Attribute class
public class DB_Attribute implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static final String SCHEMA_FORMAT = "%-30s%-20s%-10s%-10s";

	public DB_Attribute(String attrName, DataType dType, boolean na) {
		name = attrName;
		dataType = dType;
		nullable = na;
		primary = false;
		foreign = false;
		refTable = null;
		refAttribute = null;
	}

	public DB_Attribute(String attrName, DataType dType, int charlen, boolean na) {
		this(attrName, dType, na);
		charLength = charlen;
	}

	public String name;
	public DataType dataType;
	public int charLength;
	public boolean nullable;
	public boolean primary;
	public boolean foreign;
	public String refTable;
	public String refAttribute;

// Compare type with other attribute
	public boolean checkTypeMatch(DB_Attribute other) {
		boolean retval = false;
		if (dataType == other.dataType) {
			if (dataType == DataType.TYPE_CHAR) {
				if (charLength == other.charLength) {
					retval = true;
				}
			} else {
				retval = true;
			}
		}
		return retval;
	}

	@Override
	public String toString() {
		String type = null;
		switch (dataType) {
		case TYPE_INT:
			type = "int";
			break;
		case TYPE_CHAR:
			type = "char(" + charLength + ")";
			break;
		case TYPE_DATE:
			type = "date";
			break;
		}

		String key = "";
		if (primary && foreign) {
			key = "PRI/FOR";
		} else if (primary) {
			key = "PRI";
		} else if (foreign) {
			key = "FOR";
		}

		return String.format(SCHEMA_FORMAT, name, type, (nullable ? "Y" : "N"), key);
	}
}