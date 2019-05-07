package SQL;

public class MyException extends Exception {
	private static final long serialVersionUID = 1L;
	
	DBMessage msg;
	public MyException(DBMessage msg) {
		this.msg = msg;
	}
	public DBMessage getDBMessage() {
		return msg;
	}
}
