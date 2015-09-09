package pt.quintans.ezSQL.orm.app.mappings.virtual;

public class TEyeColor extends TCatalog {
	public static final TEyeColor T_EYE_COLOR = new TEyeColor();

	protected TEyeColor() {
	    super();
	    // Discriminators: enable us to give different meanings to the same table. ex: eye color, gender, ...
	    WITH(C_TYPE, "EYECOLOR");
	}
}
