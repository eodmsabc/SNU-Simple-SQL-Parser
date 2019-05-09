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

	public DBMessage createSchema(Database db, ArrayList<Attribute> colDefs, ArrayList<ArrayList<String>> primary, ArrayList<ForeignKeyConstraint> fKeyConstraints) {
		DBMessage msg;
		
		if (primary.size() > 1) {
			return new DBMessage(MsgType.DuplicatePrimaryKeyDefError);
		}

		// Column Definition
		for (Attribute attr : colDefs) {
			// Existence Check
			if (getAttribute(attr.getName()) != null) {
				return new DBMessage(MsgType.DuplicateColumnDefError);
			}
			
			if (attr.getDataType() == DataType.TYPE_CHAR) {
				// Check Negative Char Length
				if (attr.getCharLength() <= 0) {
					return new DBMessage(MsgType.CharLengthError);
				}
			}
			addAttribute(attr);
		}
		
		// Primary Key Constraints
		if (primary.size() == 1) {
			for (String col : primary.get(0)) {
				boolean colFound = setPrimary(col);
				if (colFound == false) {
					return new DBMessage(MsgType.NonExistingColumnDefError, col);
				}
			}
			pKeys = primary.get(0);
		} else {
			pKeys = new ArrayList<String>();
		}
		
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

	private ArrayList<ColValTuple> parseInputValue(ArrayList<String> colList, ArrayList<Value> valList) throws MyException {
		DBMessage msg;
		int schemaSize = schema.size();
		ArrayList<ColValTuple> retList = new ArrayList<ColValTuple>();
		for (int i = 0; i < schemaSize; i++) {
			retList.add(new ColValTuple(schema.get(i).getName()));
		}
		
		int colSize = colList.size();
		int valSize = valList.size();
		if (colSize == 0) {
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

	private DBMessage insertConstraintCheck(Database db, ArrayList<ColValTuple> cvTuple) {
		// Primary Key Constraint
		if (pKeys.size() > 0) {
			BooleanExpression pKeyCheck = generatePrimaryCheckExpr(cvTuple);
			ArrayList<ArrayList<Value>> searchResult;
			
			try {
				searchResult = pKeyCheck.filter(this);
			}
			catch (MyException e) {
				return e.getDBMessage();
			}
			
			if (searchResult.size() > 0) {
				return new DBMessage(MsgType.InsertDuplicatePrimaryKeyError);
			}
		}
		
		// Referential Integrity Check
		boolean refIntegrityCheckNeeded = true;
		for (ColValTuple cv : cvTuple) {
			if (cv.value.isNull()) {
				refIntegrityCheckNeeded = false;
				break;
			}
		}
		
		if (refIntegrityCheckNeeded) {
			for (ForeignKeyConstraint fkc : fKeys) {
				ArrayList<ColValTuple> fcv = ColValTuple.columnFilter(fkc.foreignKeys, cvTuple);
				for (int i = 0; i < fcv.size(); i++) {
					fcv.get(i).columnName = fkc.referingKeys.get(i);
				}
				
				Relation refr = Relation.db_search(db, fkc.refTable);
				BooleanExpression refbool = refr.generatePrimaryCheckExpr(fcv);
				
				ArrayList<ArrayList<Value>> fsearchList = refr.search(refbool);
				
				if (fsearchList.size() == 0) {
					return new DBMessage(MsgType.InsertReferentialIntegrityError);
				}
			}
		}
		
		return null;
	}

	public DBMessage insertRecord(Database db, ArrayList<String> colList, ArrayList<Value> valList) {
		DBMessage msg;
		ArrayList<ColValTuple> cvTuple;
		
		try {
			cvTuple = parseInputValue(colList, valList);
		}
		catch (MyException e) {
			return e.getDBMessage();
		}
		
		msg = insertConstraintCheck(db, cvTuple);
		if (msg != null) {
			return msg;
		}
		
		ArrayList<Value> newRecord = new ArrayList<Value>();
		int size = cvTuple.size();

		for (int i = 0; i < size; i++) {
			newRecord.add(cvTuple.get(i).value);
		}
		records.add(newRecord);
		
		return null;
	}

	BooleanExpression generatePrimaryCheckExpr(ArrayList<ColValTuple> cvTuple) {
		ArrayList<ColValTuple> pKeyTuple = ColValTuple.columnFilter(pKeys, cvTuple);
		
		return Relation.generateCheckExpr(pKeyTuple);
	}
	
	public DBMessage delete(Database db, BooleanExpression where) {
		int deleteCount = 0;
		int cancelCount = 0;

		ArrayList<ArrayList<Value>> searchResult;

		try {
			searchResult = where.filter(this);
		} catch (MyException e) {
			return e.getDBMessage();
		}

		for (ArrayList<Value> rec : searchResult) {
			
			ArrayList<ColValTuple> myPKey = new ArrayList<ColValTuple>();
			for (String pCol : pKeys) {
				myPKey.add(new ColValTuple(pCol, rec.get(getIndexByColumnName(pCol))));
			}
			
			// Integrity violation check
			boolean noViolation;
			
			noViolation = checkNoReferentialIntegrityViolation(db, myPKey);
			
			if (noViolation) {

				if (pKeys.size() > 0) {
					for (String refTable : referedTableList) {
						Relation refRel = db_search(db, refTable);
						refRel.cascadeDeletion(tableName, myPKey);
					}
				}
				
				records.remove(rec);
				deleteCount++;
			}
			else {
				cancelCount++;
			}
		}
		
		return new DBMessage(MsgType.DeleteResult, deleteCount, cancelCount);
	}
	
	void cascadeDeletion(String dTable, ArrayList<ColValTuple> cvList) {
		for (ForeignKeyConstraint fkc : fKeys) {
			if (fkc.refTable.equals(dTable)) {
				ArrayList<ColValTuple> myside = ColValTuple.columnFilter(fkc.referingKeys, cvList);
				for (int i = 0; i < fkc.referingKeys.size(); i++) {
					myside.get(i).columnName = fkc.foreignKeys.get(i);
				}
				
				BooleanExpression bxpr = Relation.generateCheckExpr(myside);
				ArrayList<ArrayList<Value>> searchResult = search(bxpr);
				
				for (ArrayList<Value> rec : searchResult) {
					for (String fk : fkc.foreignKeys) {
						rec.get(getIndexByColumnName(fk)).setNull();
					}
				}
			}
		}
	}
	
	boolean checkViolateIntegrity(String dTable, ArrayList<ColValTuple> cvList) {
		for (ForeignKeyConstraint fkc : fKeys) {
			if (fkc.refTable.equals(dTable) && !fkc.nullable) {
				ArrayList<ColValTuple> myside = ColValTuple.columnFilter(fkc.referingKeys, cvList);
				for (int i = 0; i < fkc.referingKeys.size(); i++) {
					myside.get(i).columnName = fkc.foreignKeys.get(i);
				}
				
				BooleanExpression bxpr = Relation.generateCheckExpr(myside);
				ArrayList<ArrayList<Value>> searchResult = search(bxpr);
				
				if (searchResult.size() > 0) {
					return true;
				}
			}
		}
		return false;
	}
	
	private boolean checkNoReferentialIntegrityViolation(Database db, ArrayList<ColValTuple> myPKey) {
		
		if (myPKey.size() == 0) {
			return true;
		}
		
		boolean violate;
		for (String referedTable : referedTableList) {
			
			Relation referedRel = db_search(db, referedTable);

			violate = referedRel.checkViolateIntegrity(tableName, myPKey);
			
			if (violate) {
				return false;
			}
		}
		
		return true;
	}
	
	public ArrayList<ArrayList<Value>> search(BooleanExpression bxpr) {
		ArrayList<ArrayList<Value>> result = null;
		try {
			result = bxpr.filter(this);
		} catch (MyException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	void select(ArrayList<Rename> selected, BooleanExpression bxpr) throws MyException {
		ArrayList<ArrayList<Value>> searchResult = null;
		
		if (bxpr == null) {
			searchResult = records;
		} else {
			searchResult = bxpr.filter(this);
		}
		
		ArrayList<Integer> indexList = new ArrayList<Integer>();
		ArrayList<ArrayList<Value>> selectedResult = selectColumn(selected, searchResult, indexList);
		selectPrintResult(indexList, selected, selectedResult);
	}
	
	private ArrayList<ArrayList<Value>> selectColumn(ArrayList<Rename> selectList, ArrayList<ArrayList<Value>> searchResult, ArrayList<Integer> indexList) {
		int schemaSize = schema.size();

		if (selectList == null) {
			for (int i = 0; i < schemaSize; i++) {
				indexList.add(i);
			}
			return searchResult;
		}

		ArrayList<ArrayList<Value>> result = new ArrayList<ArrayList<Value>>();

		String searchPattern;
		for (Rename rename : selectList) {
			searchPattern = (rename.tableName == null ? "" : rename.tableName) + "." + rename.columnName;
			for (int idx = 0; idx < schema.size(); idx++) {
				Attribute attr = schema.get(idx);
				if (Relation.lastMatch(attr.getName(), searchPattern) >= 0) {
					indexList.add(idx);
					break;
				}
			}
		}

		ArrayList<Value> entity;
		for (ArrayList<Value> rec : searchResult) {
			entity = new ArrayList<Value>();
			for (int idx : indexList) {
				entity.add(rec.get(idx));
			}
			result.add(entity);
		}

		return result;
	}

	private void selectPrintResult(ArrayList<Integer> idxList, ArrayList<Rename> selectList, ArrayList<ArrayList<Value>> selectedResult) {
		ArrayList<String> titleList = new ArrayList<String>();

		Integer[] length = new Integer[idxList.size()];

		for (int i = 0; i < idxList.size(); i++) {
			length[i] = schema.get(idxList.get(i)).getDefaultLength();
		}

		if (selectList == null) {
			for (int i = 0; i < idxList.size(); i++) {
				titleList.add(schema.get(i).getName());
				length[i] = MyCalc.max(length[i], titleList.get(i).length());
			}
		} else {
			for (int i = 0; i < selectList.size(); i++) {
				Rename rename = selectList.get(i);
				if (rename.newName != null) {
					titleList.add(rename.newName);
				} else if (rename.tableName != null) {
					titleList.add(rename.tableName + "." + rename.columnName);
				} else {
					titleList.add(rename.columnName);
				}
				length[i] = MyCalc.max(length[i], titleList.get(i).length());
			}
		}

		int resultSize = selectedResult.size();
		int columnSize = titleList.size();

		for (int i = 0; i < columnSize; i++) {
			for (int j = 0; j < resultSize; j++) {
				length[i] = MyCalc.max(length[i], selectedResult.get(j).get(i).getLength());
			}
		}

		selectRealPrint(titleList, length, selectedResult);
	}

	private static void selectRealPrint(ArrayList<String> title, Integer[] length,
			ArrayList<ArrayList<Value>> searchResult) {
		int columnNum = title.size();
		ArrayList<String> format = new ArrayList<String>();

		for (int i = 0; i < columnNum; i++) {
			String f = String.format(" %%-%ds ", length[i]);
			format.add(f);
		}

		String border = "+";
		for (int i = 0; i < columnNum; i++) {
			for (int j = 0; j < length[i] + 2; j++) {
				border = border + "-";
			}
			border = border + "+";
		}

		System.out.println(border);

		// Column Names
		System.out.print("|");
		for (int i = 0; i < columnNum; i++) {
			System.out.printf(format.get(i), title.get(i));
			System.out.print("|");
		}
		System.out.println();

		System.out.println(border);

		// Data

		for (int j = 0; j < searchResult.size(); j++) {
			System.out.print("|");
			for (int i = 0; i < columnNum; i++) {
				System.out.printf(format.get(i), searchResult.get(j).get(i));
				System.out.print("|");
			}
			System.out.println();
		}

		System.out.println(border);
	}

	private static BooleanExpression generateCheckExpr(ArrayList<ColValTuple> cvTuple) {
		Predicate p = new Predicate(cvTuple.get(0));
		BooleanNode node = new BooleanNode(p);
		
		for (int i = 1; i < cvTuple.size(); i++) {
			p = new Predicate(cvTuple.get(i));
			node = new BooleanNode('&', node, new BooleanNode(p));
		}
		
		return new BooleanExpression(node);
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

	public static DBMessage selectQuery(Database db, ArrayList<Rename> selected, ArrayList<Rename> tables, BooleanExpression bxpr) {
		DBMessage msg;

		msg = selectCheckValidTableName(db, tables);
		if (msg != null) {
			return msg;
		}

		Relation cartesian = Relation.selectJoin(db, tables);
		
		try {
			cartesian.select(selected,  bxpr);
		} catch (MyException e) {
			return e.getDBMessage();
		}

		return null;
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

	private static DBMessage selectCheckValidTableName(Database db, ArrayList<Rename> tables) {
		ArrayList<String> newNameDupCheck = new ArrayList<String>();

		for (Rename r : tables) {
			Relation rel = Relation.db_search(db, r.tableName);
			if (rel == null) {
				return new DBMessage(MsgType.SelectTableExistenceError, r.tableName);
			}

			if (newNameDupCheck.contains(r.newName)) {
				// TODO return some error
				return null;
			} else {
				newNameDupCheck.add(r.newName);
			}
		}
		return null;
	}

	private static Relation selectJoin(Database db, ArrayList<Rename> tables) {
		Relation result = new Relation("--result");
		Relation rtemp;

		for (Rename r : tables) {
			rtemp = Relation.db_search(db, r.tableName);
			result = Relation.join(result, null, rtemp, r.newName);
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
