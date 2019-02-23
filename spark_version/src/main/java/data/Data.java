package data;
import java.io.Serializable;

public interface Data<T> extends Serializable, Cloneable{
	
	T add(T data);
	T devide(Integer n);
	Double distance(T data);
}
