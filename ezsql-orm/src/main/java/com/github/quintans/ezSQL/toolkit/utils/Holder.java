package com.github.quintans.ezSQL.toolkit.utils;

public class Holder<T>{
	private T object;
	
	public Holder() {
	}
	
	public Holder(T object) {
		this.object = object;
	}

	public T get() {
		return object;
	}

	public void set(T object) {
		this.object = object;
	}
	
	public boolean isEmpty(){
		return object == null;
	}

	public String toString(){
		if(object != null)
			return object.toString();
		else
			return null;
	}

}
