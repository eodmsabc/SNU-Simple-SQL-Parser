package SQL;

import java.io.Serializable;
import java.util.ArrayList;

public class Relation implements Serializable {
	private static final long serialVersionUID = 1L;

	private String tableName;
	private ArrayList<Attribute> schema;
	private ArrayList<String> referedTableList;
	private ArrayList<ArrayList<Value>> records;

	public Relation(String tableName) {
		this.tableName = tableName;
		schema = new ArrayList<Attribute>();
		referedTableList = new ArrayList<String>();
		records = new ArrayList<ArrayList<Value>>();
	}

	public String getTableName() {
		return tableName;
	}

	ArrayList<Attribute> getSchema() {
		return schema;
	}

	public ArrayList<String> getReferedTableList() {
		return referedTableList;
	}

	public ArrayList<ArrayList<Value>> getRecords() {
		return records;
	}

	public Attribute getAttribute(String colName) {
		for (Attribute attr : schema) {
			if (attr.getName().equals(colName)) {
				return attr;
			}
		}
		return null;
	}

	public ArrayList<String> getColumnList() {
		ArrayList<String> colList = new ArrayList<String>();
		for (Attribute attr : schema) {
			colList.add(attr.getName());
		}
		return colList;
	}
	
	public ArrayList<String> getTableReferingColumns(String refTable) {
		ArrayList<String> refColList = new ArrayList<String>();
		
		for (Attribute attr : schema) {
			if (refTable.equals(attr.getRefTable())) {
				refColList.add(attr.getName());
			}
		}
		
		return refColList;
	}
	
	public String getColumnNameByNumber(int col) {
		if (col < 0 || schema.size() <= col) return null;
		else return schema.get(col).getName();
	}
	
	public DBMessage createSchema(ArrayList<Parse_TableElement> colDefs, ArrayList<String> primaryKeys) {
		// Column Definition
		for (Parse_TableElement elem : colDefs) {
			// Existence Check
			if (getAttribute(elem.columnName) != null) {
				return new DBMessage(MsgType.DuplicateColumnDefError);
			}

			Attribute newAttribute;
			if (elem.dataType == DataType.TYPE_CHAR) {
				// Check Negative Char Length
				if (elem.charlen <= 0) {
					return new DBMessage(MsgType.CharLengthError);
				}
				newAttribute = new Attribute(elem.columnName, elem.dataType, elem.charlen, elem.nullable);
			} else {
				newAttribute = new Attribute(elem.columnName, elem.dataType, elem.nullable);
			}
			addAttribute(newAttribute);
		}

		// Primary Key Constraints
		for (String col : primaryKeys) {
			boolean colFound = setPrimary(col);
			if (colFound == false) {
				return new DBMessage(MsgType.NonExistingColumnDefError, col);
			}
		}
		return null;
	}

	private void addAttribute(Attribute attr) {
		schema.add(attr);
	}

	public ArrayList<String> getPrimaryKeys() {
		ArrayList<String> primaryKeys = new ArrayList<String>();
		
		for (Attribute attr : schema) {
			if (attr.isPrimary()) {
				primaryKeys.add(attr.getName());
			}
		}
		
		return primaryKeys;
	}
	
	private boolean setPrimary(String colName) {
		for (Attribute attr : schema) {
			if (colName.equals(attr.getName())) {
				attr.setPrimary();
				return true;
			}
		}
		return false;
	}

	public ArrayList<String> getForeignKeys() {
		ArrayList<String> foreignKeys = new ArrayList<String>();
		
		for (Attribute attr : schema) {
			if (attr.isForeign()) {
				foreignKeys.add(attr.getName());
			}
		}
		
		return foreignKeys;
	}
	
	public boolean setForeign(String colName, String refTable, String refCol) {
		for (Attribute attr : schema) {
			if (colName.equals(attr.getName())) {
				attr.setForeign(refTable, refCol);
				return true;
			}
		}
		return false;
	}
	
	public String getRefTable(String foreignKey) {
		String refTable = null;
		for (Attribute attr : schema) {
			if (foreignKey.equals(attr.getName())) {
				refTable = attr.getRefTable();
			}
		}
		return refTable;
	}
	
	public ArrayList<String> getReferingTableList() {
		ArrayList<String> referingTableList = new ArrayList<String>();
		for (Attribute attr : schema) {
			String refTableName = attr.getRefTable();
			if (!referingTableList.contains(refTableName)) {
				referingTableList.add(refTableName);
			}
		}
		return referingTableList;
	}
	
	public void addReferedTableList(String rTable) {
		if (referedTableList.contains(rTable))
			return;
		referedTableList.add(rTable);
	}

	public void delReferedTableList(String rTable) {
		if (!referedTableList.contains(rTable))
			return;
		referedTableList.remove(rTable);
	}

	public int getRefCount() {
		return referedTableList.size();
	}

	public String describe() {
		String desc = "table_name [" + tableName + "]\n";
		desc += "-------------------------------------------------\n";
		desc += String.format(Attribute.SCHEMA_FORMAT + "\n", "column_name", "type", "null", "key");
		for (Attribute attr : schema) {
			desc += attr.toString() + "\n";
		}
		desc += "-------------------------------------------------\n";
		
		return desc;
	}
		
	public DBMessage insertConstraintsCheck(Parse_InsertValue insertVal, ArrayList<ValueCompare> vcList) {
		DBMessage msg;
		int size = insertVal.valList.size();
		int schemaSize = schema.size();
		
		if (insertVal.colList == null) {
			if (size != schemaSize) {
				return new DBMessage(MsgType.InsertTypeMismatchError);
			}
			insertVal.colList = getColumnList();
		}
		else if (size != insertVal.colList.size()){
			return new DBMessage(MsgType.InsertTypeMismatchError);
		}
		
		// Validate input column names
		for (int index = 0; index < size; index++) {
			String colName = insertVal.colList.get(index);
			
			Attribute attr = getAttribute(colName);
			
			if (attr == null) {
				return new DBMessage(MsgType.InsertColumnExistenceError, colName);
			}

			// Check Column Name Duplication
			for (int index2 = index + 1; index2 < size; index2++) {
				if (colName.equals(insertVal.colList.get(index2))) {
					return new DBMessage(MsgType.InsertTypeMismatchError);
				}
			}
		}
		
		// Construct new value list w/ same order with schema
		ArrayList<String> colNameList = getColumnList();
		for (String col : colNameList) {
			Value val = insertVal.getValue(col);
			if (val == null) val = new Value();
			
			vcList.add(new ValueCompare(tableName, col, val, Comparator.EQ));
		}
		
		// Check Types
		for (ValueCompare vc : vcList) {
			Attribute attr = getAttribute(vc.columnName);
			msg = attr.typeCheck(vc.value);
			if (msg != null) {
				return msg;
			}
		}
		
		// Check primary key constraints
		ArrayList<String> primaryKeys = getPrimaryKeys();
		ArrayList<ValueCompare> primaryValues = ValueCompare.vcColumnFilter(vcList, primaryKeys);
		
		ArrayList<ArrayList<Value>> result = new ArrayList<ArrayList<Value>>();
		msg = select(primaryValues, result);
		
		if (msg != null) {
			// TODO DEBUG This code should not be executed
			System.out.println("This should not be displayed. Insert Primary Search Error");
		}
		
		if (result.size() != 0) {
			return new DBMessage(MsgType.InsertDuplicatePrimaryKeyError);
		}
		
		return null;
	}
	
	public void insertRecord(ArrayList<ValueCompare> insertRecord) {
		ArrayList<Value> newRecord = new ArrayList<Value>();
		int size = insertRecord.size();
		
		for (int i = 0; i < size; i++) {
			Value val = insertRecord.get(i).value;
			newRecord.add(val);
		}
		records.add(newRecord);
	}
	
	public DBMessage select(ArrayList<ValueCompare> where, ArrayList<ArrayList<Value>> result) {
		ArrayList<Integer> colIndexList = new ArrayList<Integer>();
		
		int size = where.size();
		for (int i = 0; i < size; i++) {
			ValueCompare vc = where.get(i);
			if (vc.tableName.equals(tableName)) {
				int index = schema.indexOf(getAttribute(vc.columnName));
				if (index < 0) {
					return new DBMessage(MsgType.SelectColumnResolveError, vc.columnName);
				}
				colIndexList.add(index);
			}
		}
		
		boolean matched;
		for (ArrayList<Value> record : records) {
			for (int i = 0; i < size; i++) {
				matched = true;
				if (!where.get(i).check(record.get(colIndexList.get(i)))) {
					matched = false;
				}
				if (matched) {
					result.add(record);
				}
			}
		}
		
		return null;
	}
	
	public static DBMessage referenceCheck(Relation cur, Relation ref, Parse_TableElement elem) {
		int keySize = elem.foreign.size();

		for (int index = 0; index < keySize; index++) {
			String curCol = elem.foreign.get(index);
			String refCol = elem.refKeys.get(index);

			Attribute curAttr = cur.getAttribute(curCol);
			if (curAttr == null) {
				return new DBMessage(MsgType.NonExistingColumnDefError, curCol);
			}

			Attribute refAttr = ref.getAttribute(refCol);
			if (refAttr == null) {
				return new DBMessage(MsgType.ReferenceColumnExistenceError);
			}

			if (!refAttr.isPrimary()) {
				return new DBMessage(MsgType.ReferenceNonPrimaryKeyError);
			}

			if (!Attribute.checkTypeMatch(curAttr, refAttr)) {
				return new DBMessage(MsgType.ReferenceTypeError);
			}
			
			cur.setForeign(curCol, ref.getTableName(), refCol);
		}
		
		// Check if FK references all primary key set
		ArrayList<Attribute> refSchema = ref.getSchema();
		
		for (Attribute attr : refSchema)
		{
			if (attr.isPrimary()) {
				if (!elem.refKeys.contains(attr.getName())) {
					return new DBMessage(MsgType.ReferenceNonPrimaryKeyError);
				}
			}
		}
		
		return null;
	}
}

class Attribute implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final String SCHEMA_FORMAT = "%-30s%-20s%-10s%-10s";

	private String name;
	private DataType dataType;
	private int charLength;
	private boolean nullable;
	private boolean primary;
	private boolean foreign;
	private String refTable;
	private String refColumn;

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

	public String getName() {
		return name;
	}

	public DataType getDataType() {
		return dataType;
	}

	public int getCharLength() {
		return charLength;
	}

	public boolean isNullable() {
		return nullable;
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
			val.type = dataType;
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
