package pt.quintans.ezSQL.exceptions;

public class OptimisticLockException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public OptimisticLockException(String msg) {
		super(msg);		
	}

	public OptimisticLockException(String msg, Throwable t) {
		super(msg, t);
	}

	public OptimisticLockException(Throwable t) {
		super(t);		
	}
}
