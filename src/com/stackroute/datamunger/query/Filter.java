package com.stackroute.datamunger.query;

//this class contains methods to evaluate expressions
public class Filter {
	public boolean evaluateExpression(String operator, String firstInput, String secondInput, String dataType) {
		switch (operator) {
		case "=":
			if (equalTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case "!=":
			if (!equalTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case ">=":
			if (greaterThanOrEqualTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case "<=":
			if (lessThanOrEqualTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case ">":
			if (greaterThan(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case "<":
			if (lessThan(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		}
		return false;
	}
	
	public boolean equalTo(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":
			
		case "java.lang.Double":
			
		default:
			if (firstInput.equalsIgnoreCase(secondInput))
				return true;
			else
				return false;
		}
	}
	public boolean greaterThan(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":
			
		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) > Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (NumberFormatException nfe) {
				return false;
			}
		default:
			if ((firstInput.compareToIgnoreCase(secondInput)) > 0)
				return true;
			else
				return false;
		}
	}
	public boolean greaterThanOrEqualTo(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":
			
		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) >= Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (NumberFormatException nfe) {
				return false;
			}
		default:
			if ((firstInput.compareToIgnoreCase(secondInput)) >= 0)
				return true;
			else
				return false;
		}
	}
	public boolean lessThan(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":
			
		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) < Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (NumberFormatException nfe) {
				return false;
			}
		default:
			if ((firstInput.compareToIgnoreCase(secondInput)) < 0)
				return true;
			else
				return false;
		}
	}
	public boolean lessThanOrEqualTo(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":
			
		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) <= Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (NumberFormatException nfe) {
				return false;
			}
		default:
			if ((firstInput.compareToIgnoreCase(secondInput)) <= 0)
				return true;
			else
				return false;
		}
	}
}
