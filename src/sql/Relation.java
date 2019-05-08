package sql;

import java.io.Serializable;
import java.util.ArrayList;
import com.sleepycat.je.Database;

public class Relation implements Serializable {
	private static final long serialVersionUID = 1L;

	private String tableName;
	private ArrayList<Attribute> schema;
	private ArrayList<String> referedTableList;
	private ArrayList<ArrayList<Value>> records;
	private ArrayList<String> pKeys;
	private ArrayList<ForeignKeyConstraint> fKeys;

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

	public ArrayList<String> getPrimaryKeys() {
		return pKeys;
	}

	public ArrayList<ForeignKeyConstraint> getForeignKeyConstraint() {
		return fKeys;
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

	public DBMessage createSchema(Database db, ArrayList<Parse_Create> elements) {
		DBMessage msg;
		ArrayList<Parse_Create> colDefs = new ArrayList<Parse_Create>();
		ArrayList<String> primaryKeys = null;
		ArrayList<ForeignKeyConstraint> fKeyConstraints = new ArrayList<ForeignKeyConstraint>();

		boolean pKeyAppeared = false;

		for (Parse_Create elem : elements) {
			switch (elem.type) {
			case COLDEF:
				colDefs.add(elem);
				break;

			case PRIMARY:
				// Duplicated Primary Key Definition
				if (pKeyAppeared) {
					return new DBMessage(MsgType.DuplicatePrimaryKeyDefError);
				}
				pKeyAppeared = true;
				primaryKeys = elem.primary;
				break;

			case FOREIGN:
				fKeyConstraints.add(elem.foreign);
				break;
			}
		}
		
		// Column Definition
		for (Parse_Create elem : colDefs) {
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
		if (primaryKeys != null) {
			for (String col : primaryKeys) {
				boolean colFound = setPrimary(col);
				if (colFound == false) {
					return new DBMessage(MsgType.NonExistingColumnDefError, col);
				}
			}
		}
		pKeys = primaryKeys;
		
		// Foreign Key Constraints
		Relation foreignRelation;
		for (ForeignKeyConstraint elem : fKeyConstraints) {
			foreignRelation = db_search(db, elem.refTable);

			if (foreignRelation == null) {
				return new DBMessage(MsgType.ReferenceTableExistenceError);
			}

			// Check the count of reference column
			int forKeySize = elem.foreignKeys.size();
			int refKeySize = elem.referingKeys.size();
			if (forKeySize != refKeySize) {
				return new DBMessage(MsgType.ReferenceTypeError);
			}

			msg = referenceCheck(foreignRelation, elem);

			if (msg != null) {
				return msg;
			}
		}
		fKeys = fKeyConstraints;
		
		// Modify other relation's referenceList & nullable set
		for (ForeignKeyConstraint fkc : fKeys) {
			boolean nullable = true;
			for (String fcol : fkc.foreignKeys) {
				nullable = nullable && getAttribute(fcol).isNullable();
			}
			fkc.nullable = nullable;
			addReferenceList(db, fkc.refTable);
		}

		return null;
	}

	public DBMessage referenceCheck(Relation foreign, ForeignKeyConstraint elem) {
		int keySize = elem.foreignKeys.size();

		for (int index = 0; index < keySize; index++) {
			String curCol = elem.foreignKeys.get(index);
			String refCol = elem.referingKeys.get(index);

			Attribute curAttr = getAttribute(curCol);
			if (curAttr == null) {
				return new DBMessage(MsgType.NonExistingColumnDefError, curCol);
			}

			Attribute refAttr = foreign.getAttribute(refCol);
			if (refAttr == null) {
				return new DBMessage(MsgType.ReferenceColumnExistenceError);
			}

			if (!refAttr.isPrimary()) {
				return new DBMessage(MsgType.ReferenceNonPrimaryKeyError);
			}

			if (!Attribute.checkTypeMatch(curAttr, refAttr)) {
				return new DBMessage(MsgType.ReferenceTypeError);
			}

			setForeign(curCol, foreign.getTableName(), refCol);
		}

		// Check if FK references all primary key set
		ArrayList<Attribute> refSchema = foreign.getSchema();

		for (Attribute attr : refSchema) {
			if (attr.isPrimary()) {
				if (!elem.referingKeys.contains(attr.getName())) {
					return new DBMessage(MsgType.ReferenceNonPrimaryKeyError);
				}
			}
		}

		return null;
	}

	private void addAttribute(Attribute attr) {
		schema.add(attr);
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

	boolean setForeign(String colName, String refTable, String refCol) {
		Attribute attr = getAttribute(colName);
		if (attr == null) {
			return false;
		}

		attr.setForeign(refTable, refCol);
		return true;
	}

	String getRefTableOfKey(String foreignKey) {
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
		for (ForeignKeyConstraint fkc : fKeys) {

			String refTableName = fkc.refTable;
			if (!referingTableList.contains(refTableName)) {
				referingTableList.add(refTableName);
			}
		}
		return referingTableList;
	}

	private void addReferenceList(Database db, String target) {
		Relation targetRel = Relation.db_search(db, target);
		if (targetRel == null)
			return;
		targetRel.addReferedTableList(tableName);
		Relation.db_replace(db, targetRel);
	}

	private void delReferenceList(Database db, String target) {
		Relation targetRel = Relation.db_search(db, target);
		if (targetRel == null)
			return;
		targetRel.delReferedTableList(tableName);
		Relation.db_replace(db, targetRel);
	}

	void addReferedTableList(String rTable) {
		if (referedTableList.contains(rTable))
			return;
		referedTableList.add(rTable);
	}

	void delReferedTableList(String rTable) {
		if (!referedTableList.contains(rTable))
			return;
		referedTableList.remove(rTable);
	}

	int getRefCount() {
		return referedTableList.size();
	}

	public DBMessage dropCleanUp(Database db) {
		if (getRefCount() > 0) {
			return new DBMessage(MsgType.DropReferencedTableError, tableName);
		}

		ArrayList<String> refTableList = getReferingTableList();
		for (String refTable : refTableList) {
			delReferenceList(db, refTable);
		}
		return null;
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

	public ArrayList<ColValTuple> parseInputValue(Parse_Insert insertVal) throws MyException {
		DBMessage msg;
		int schemaSize = schema.size();
		ArrayList<ColValTuple> retList = new ArrayList<ColValTuple>();
		for (int i = 0; i < schemaSize; i++) {
			retList.add(new ColValTuple(schema.get(i).getName()));
		}
		
		ArrayList<Value> valList = insertVal.valList;
		ArrayList<String> colList = insertVal.colList;
		int colSize;
		int valSize = insertVal.valList.size();
		if (colList == null) {
			if (valSize != schemaSize) {
				throw new MyException(MsgType.InsertTypeMismatchError);
			}
			else {
				for (int i = 0; i < schemaSize; i++) {
					// Type Check
					msg = schema.get(i).typeCheck(valList.get(i));
					if (msg != null) {
						throw new MyException(msg);
					}
					retList.get(i).value = valList.get(i);
				}
			}
		}
		else {
			colSize = colList.size();
			if (colSize != valSize) {
				throw new MyException(MsgType.InsertTypeMismatchError);
			}
			else {
				// Column Existence Check
				ArrayList<String> columnCheck = getColumnList();
				for (String c : colList) {
					if (!columnCheck.contains(c)) {
						throw new MyException(MsgType.InsertColumnExistenceError, c);
					}
				}
				
				for (int i = 0; i < schemaSize; i++) {
					int cListIdx = colList.indexOf(columnCheck.get(i));
					if (cListIdx < 0) {	// Null check
						if (!schema.get(i).isNullable()) {
							throw new MyException(MsgType.InsertTypeMismatchError);
						}
						retList.get(i).value = new Value();
					}
					else {	// Type Check
						msg = schema.get(i).typeCheck(valList.get(cListIdx));
						if (msg != null) {
							throw new MyException(msg);
						}
						retList.get(i).value = valList.get(cListIdx);
					}
				}
			}
		}
		
		return retList;
	}
	
	public ArrayList<ColValTuple> insertParse(Database db, Parse_Insert insertVal) throws MyException {
		
		ArrayList<ColValTuple> cvTuple;
		
		try {
			cvTuple = parseInputValue(insertVal);
		}
		catch (MyException e) {
			throw e;
		}
		
		if (pKeys != null) {
			BooleanExpression pKeyCheck = generatePrimaryCheckExpr(cvTuple);
			
			ArrayList<ArrayList<Value>> searchResult = pKeyCheck.filter(this);
			if (searchResult.size() > 0) {
				throw new MyException(MsgType.InsertDuplicatePrimaryKeyError);
			}
		}
		
		return cvTuple;
	}

	public void insertRecord(ArrayList<ColValTuple> cvTuple) {
		ArrayList<Value> newRecord = new ArrayList<Value>();
		int size = cvTuple.size();

		for (int i = 0; i < size; i++) {
			newRecord.add(cvTuple.get(i).value);
		}
		records.add(newRecord);
	}

	private BooleanExpression generatePrimaryCheckExpr(ArrayList<ColValTuple> cvTuple) {
		int index = getIndexByColumnName(pKeys.get(0));
		Predicate p = new Predicate(cvTuple.get(index));
		BooleanNode node = new BooleanNode(p);
		
		for (int i = 1; i < pKeys.size(); i++) {
			index = getIndexByColumnName(pKeys.get(i));
			p = new Predicate(cvTuple.get(index));
			node = new BooleanNode('&', node, new BooleanNode(p));
		}
		
		return new BooleanExpression(node);
	}
	
	public DBMessage select(ArrayList<ColValTuple> where, ArrayList<ArrayList<Value>> result) {
		ArrayList<Integer> colIndexList = new ArrayList<Integer>();

		int size = where.size();
		for (int i = 0; i < size; i++) {
			ColValTuple vc = where.get(i);
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

	public static int lastMatch(String orig, String pattern) {
		int idx = orig.indexOf(pattern);

		if (idx < 0)
			return -1;

		if (orig.substring(idx).equals(pattern)) {
			return idx;
		} else {
			return -1;
		}
	}

	/*
	 * public static Relation join(Relation r1, Relation r2) { return
	 * Relation.join(r1, r1.getTableName(), r2, r2.getTableName()); }
	 * 
	 * public static Relation join(Relation r1, String newTable1, Relation r2) {
	 * return Relation.join(r1, newTable1, r2, r2.getTableName()); }
	 * 
	 * public static Relation join(Relation r1, Relation r2, String newTable2) {
	 * return Relation.join(r1, r1.getTableName(), r2, newTable2); }
	 */
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

	public static void db_insert(Database db, Relation r) {
		DataManager.insert(db, r.getTableName(), DataManager.serialize(r));
	}

	public static void db_replace(Database db, Relation r) {
		DataManager.replace(db, r.getTableName(), DataManager.serialize(r));
	}

	public static Relation db_search(Database db, String table) {
		byte[] data = DataManager.search(db, table);
		Relation rel = null;
		if (data != null) {
			rel = (Relation) DataManager.deserialize(data);
		}
		return rel;
	}

	public static void db_delete(Database db, Relation r) {
		DataManager.delete(db, r.getTableName());
	}
}
