package sql;

import java.io.Serializable;

public class Attribute implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final String SCHEMA_FORMAT = "%-30s%-20s%-10s%-10s";

	private String name;
	private String fullName;
	private DataType dataType;
	private int charLength;
	private boolean nullable;
	private boolean primary;
	private boolean foreign;
	private String refTable;
	private String refColumn;

	public Attribute(String attrName) {
		name = attrName;
		nullable = true;
	}
	
	public Attribute(String attrName, DataType dataType, boolean nullable) {
		name = attrName;
		this.dataType = dataType;
		this.nullable = nullable;
		primary = false;
		foreign = false;
		refTable = null;
		refColumn = null;
	}

	public Attribute(String attrName, DataType dataType, int charlen, boolean nullable) {
		this(attrName, dataType, nullable);
		charLength = charlen;
	}
	
	public Attribute copyAttribute() {
		Attribute attr = new Attribute(this.name);
		attr.fullName = this.fullName;
		attr.dataType = this.dataType;
		attr.charLength = this.charLength;
		attr.nullable = this.nullable;
		attr.primary = this.primary;
		attr.foreign = this.foreign;
		attr.refTable = this.refTable;
		attr.refColumn = this.refColumn;
		return attr;
	}

	public String getName() {
		return name;
	}
	
	public void setFullName(String tableName) {
		fullName = tableName + "." + name;
	}
	
	public String getFullName() {
		return fullName;
	}
	
	public String getTableName() {
		int idx = fullName.indexOf('.');
		if (idx < 0) return null;
		else return fullName.substring(0, idx);
	}
	
	public boolean nameMatch(String col) {
		if (col.indexOf('.') < 0) {
			return name.equals(col);
		}
		else {
			return fullName.equals(col);
		}
	}

	public DataType getDataType() {
		return dataType;
	}
	
	public void setDataType(DataType d) {
		dataType = d;
	}

	public int getCharLength() {
		return charLength;
	}
	
	public void setCharLength(int cl) {
		charLength = cl;
	}

	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean n) {
		nullable = n;
	}
	
	public boolean isPrimary() {
		return primary;
	}
	
	public void setPrimary() {
		primary = true;
		nullable = false;
	}

	public boolean isForeign() {
		return foreign;
	}
	
	public void setForeign(String refTable, String refCol) {
		foreign = true;
		this.refTable = refTable;
		this.refColumn = refCol;
	}
	
	public String getRefTable() {
		return refTable;
	}

	public String getRefColumn() {
		return refColumn;
	}

	public int getDefaultLength() {
		switch(dataType) {
		case TYPE_CHAR:
			return charLength;
		case TYPE_DATE:
			return 10;
		default:
			return 1;
		}
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

	public DBMessage typeCheck(Value val) {
		if (val.isNull()) {
			if (!nullable) {
				return new DBMessage(MsgType.InsertColumnNonNullableError, name);
			}
			return null;
		}
		
		if (dataType != val.type) {
			return new DBMessage(MsgType.InsertTypeMismatchError);
		}
		
		if (dataType == DataType.TYPE_CHAR) {
			if (val.strVal.length() != charLength) {
				return new DBMessage(MsgType.InsertTypeMismatchError);
			}
		}
		
		return null;
	}
	
	public static boolean checkTypeMatch(Attribute a, Attribute b) {
		if (a.getDataType() == b.getDataType()) {
			if (a.getDataType() == DataType.TYPE_CHAR) {
				if (a.getCharLength() == b.getCharLength()) {
					return true;
				}
			} else {
				return true;
			}
		}
		return false;
	}
	
}