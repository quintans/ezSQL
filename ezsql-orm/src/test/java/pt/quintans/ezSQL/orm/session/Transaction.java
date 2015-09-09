package pt.quintans.ezSQL.orm.session;

public class Transaction {
	private java.sql.Connection connection;
	private String user;
	private String session;
	private boolean running = true;
	private int depth;

	public java.sql.Connection getConnection() {
		return connection;
	}

	public void setConnection(java.sql.Connection connection) {
		this.connection = connection;
		running = true;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}	

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}

	public void clear() {
		connection = null;
		session = null;
		user = null;
		running = false;
		depth = 0;
    }

	public int getDepth() {
		return depth;
	}     
    
	public int incDepth(){
		return ++depth;
	}

	public int decDepth(){
		return --depth;
	}
	
	public boolean isInTransaction(){
		return connection != null && running && depth > 0;
	}
}
