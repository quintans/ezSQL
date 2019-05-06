package com.github.quintans.ezSQL.orm.app.domain;

import com.github.quintans.ezSQL.transformers.Convert;

import java.awt.*;
import java.util.Date;

public class Employee extends BaseDomain<Long> {

	private String name;
	private Boolean sex;
	private EPayGrade payGrade;
	private Date creation;

	@Convert(ColorConverter.class)
	private Color eyeColor;

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
        dirty("name");
		this.name = name;
	}

	public Boolean getSex() {
		return this.sex;
	}

	public void setSex(Boolean sex) {
        dirty("sex");
		this.sex = sex;
	}

	public EPayGrade getPayGrade() {
        return payGrade;
    }

    public void setPayGrade(EPayGrade payGrade) {
		dirty("payGrade");
		this.payGrade = payGrade;
    }

    public Date getCreation() {
        return creation;
    }

    public void setCreation(Date creation) {
        dirty("creation");
        this.creation = creation;
    }

	public Color getEyeColor() {
		return eyeColor;
	}

	public void setEyeColor(Color eyeColor) {
		dirty("eyeColor");
		this.eyeColor = eyeColor;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Artist [id=");
		builder.append(this.id);
		builder.append(", name=");
		builder.append(this.name);
		builder.append(", sex=");
		builder.append(this.sex);
        builder.append(", payGrade=");
        builder.append(this.payGrade);
		builder.append(", birth=");
		builder.append(this.creation);
		builder.append("]");
		return builder.toString();
	}

}
