package pt.quintans.ezSQL.dml;

public class EFunction {
	public static final String COLUMN = "COLUMN";
	// CONDITIONS
	public static final String EQ = "EQ";
	public static final String NEQ = "NEQ";
	public static final String GT = "GT";
	public static final String LT = "LT";
	public static final String GTEQ = "GTEQ";
	public static final String LTEQ = "LTEQ";
	public static final String LIKE = "LIKE";

	public static final String IEQ = "IEQ";
	public static final String IGT = "IGT";
	public static final String ILT = "ILT";
	public static final String IGTEQ = "IGTEQ";
	public static final String ILTEQ = "ILTEQ";
	public static final String ILIKE = "ILIKE";

	public static final String IN = "IN";
	public static final String RANGE = "RANGE";
	public static final String VALUERANGE = "VALUERANGE";
	public static final String BOUNDEDRANGE = "BOUNDEDRANGE";
	public static final String ISNULL = "ISNULL";
	public static final String OR = "OR";
	public static final String AND = "AND";

	public static final String EXISTS = "EXISTS";
	public static final String NOT = "NOT";

	// FUNCTIONS
	public static final String PARAM = "PARAM"; // parameter
	public static final String RAW = "RAW"; // sets a predefined value
	public static final String ASIS = "VAL"; // value is injected to the SQL as is.
	public static final String ALIAS = "ALIAS";
	public static final String COUNT = "COUNT";
	public static final String SUM = "SUM";
	public static final String MAX = "MAX";
	public static final String MIN = "MIN";
	public static final String RTRIM = "RTRIM";
	public static final String NOW = "NOW";

	public static final String MULTIPLY = "MULTIPLY";
	public static final String DIVIDE = "DIVIDE";
	public static final String ADD = "ADD";
	public static final String SECONDSDIFF = "SECONDSDIFF";
	public static final String MINUS = "MINUS";

	public static final String SUBQUERY = "SUBQUERY";
	public static final String AUTONUM = "AUTONUM"; // gera numero para uma coluna

    public static final String UPPER = "UPPER";
    public static final String LOWER = "LOWER";
	public static final String COALESCE = "COALESCE";
}
