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

	public ArrayList<Attribute> getSchema() {
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
	
	public int getIndexByColumnName(String col) {
		int size = schema.size();
		for (int idx = 0; idx < size; idx++) {
			Attribute attr = schema.get(idx);
			if (col.equals(attr.getName())) {
				return idx;
			}
		}
		return -1;
	}
	
	/*
	public String getColumnNameByIndex(int idx) {
		if (idx < 0 || schema.size() <= idx) return null;
		else return schema.get(idx).getName();
	}
	*/
	
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
		
		if (primaryKeys == null) {
			return null;
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
			if (!attr.isForeign()) continue;
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
	
	public Value getValue(ArrayList<Value> rec, String columnName) {
		int idx = getIndexByColumnName(columnName);
		return rec.get(idx);
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
	
	public static int lastMatch(String orig, String pattern) {
		int idx = orig.indexOf(pattern);
		
		if (idx < 0) return -1;
		
		if (orig.substring(idx).equals(pattern)) {
			return idx;
		}
		else {
			return -1;
		}
	}
	
	public static Relation join(Relation r1, Relation r2) {
		return Relation.join(r1, r1.getTableName(), r2, r2.getTableName());
	}
	public static Relation join(Relation r1, String newTable1, Relation r2) {
		return Relation.join(r1, newTable1, r2, r2.getTableName());
	}
	public static Relation join(Relation r1, Relation r2, String newTable2) {
		return Relation.join(r1, r1.getTableName(), r2, newTable2);
	}
	public static Relation join(Relation r1, String newTable1, Relation r2, String newTable2) {
		
		Relation result = new Relation("--result");
		ArrayList<ArrayList<Value>> newRecord = result.getRecords();
		ArrayList<Attribute> schema = result.getSchema();
		
		if (newTable1 == null) {
			newTable1 = r1.getTableName();
		}
		
		if (newTable2 == null) {
			newTable2 = r2.getTableName();
		}
		
		// Generate Schema
		ArrayList<Attribute> schema1 = r1.getSchema();
		ArrayList<Attribute> schema2 = r2.getSchema();
		int ssize1 = schema1.size();
		int ssize2 = schema2.size();
		String temp;
		
		for (int i = 0; i < ssize1; i++) {
			temp = schema1.get(i).getName();
			if (r1.getTableName().charAt(0) != '-') {
				temp = newTable1 + "." + temp;
			}
			schema.add(schema1.get(i).copyAttribute(temp));
		}
		for (int j = 0; j < ssize2; j++) {
			temp = schema2.get(j).getName();
			if (r2.getTableName().charAt(0) != '-') {
				temp = newTable2 + "." + temp;
			}
			schema.add(schema2.get(j).copyAttribute(temp));
		}
		
		// Generate Record Table
		ArrayList<ArrayList<Value>> record1 = r1.getRecords();
		ArrayList<ArrayList<Value>> record2 = r2.getRecords();
		int rsize1 = record1.size();
		int rsize2 = record2.size();
		
		ArrayList<Value> newEntity;
		
		if (rsize1 == 0) {
			for (int j = 0; j < rsize2; j++) {
				newEntity = new ArrayList<Value>();
				newEntity.addAll(record2.get(j));
				newRecord.add(newEntity);
			}
			return result;
		}
		
		for (int i = 0; i < rsize1; i++) {
			for (int j = 0; j < rsize2; j++) {
				newEntity = new ArrayList<Value>();
				newEntity.addAll(record1.get(i));
				newEntity.addAll(record2.get(j));
				newRecord.add(newEntity);
			}
		}
		
		return result;
	}
}
