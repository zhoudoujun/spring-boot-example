package com.adou.example.entity;

import java.util.Date;

public class Student {
	private String name;
	private Integer age;
	private Date birthDay;
	private Double weight;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public Date getBirthDay() {
		return birthDay;
	}
	public void setBirthDay(Date birthDay) {
		this.birthDay = birthDay;
	}
	public Double getWeight() {
		return weight;
	}
	public void setWeight(Double weight) {
		this.weight = weight;
	}
	@Override
	public String toString() {
		return "Student [name=" + name + ", age=" + age + ", birthDay=" + birthDay + ", weight=" + weight + "]";
	}
}
