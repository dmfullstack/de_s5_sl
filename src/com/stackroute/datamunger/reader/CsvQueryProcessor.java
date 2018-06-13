package com.stackroute.datamunger.reader;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

public class CsvQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public DataSet getResultSet(QueryParameter queryParameter) throws FileNotFoundException {
		BufferedReader reader = null;
		long rowid = 1;
		String[] headers = null;
		String[] firstRowValues = null;
		String rowValue = null;
		String[] rowValues = null;
		DataSet dataSet = new DataSet();
		Header header = new Header();
		Row row = new Row();
		RowDataTypeDefinitions dataTypeDef = new RowDataTypeDefinitions();
		Filter filter = new Filter();
		int columnSequence = 1;
		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */
		try {
			reader = new BufferedReader(new FileReader(queryParameter.getFile()));
		} catch (FileNotFoundException e) {
			System.out.println("File with name " + queryParameter.getFile() + " does not exist");
		}
		try {
			/*
			 * read the first line which contains the header. Please note that the headers
			 * can contain spaces in between them. For eg: city, winner
			 */
			headers = reader.readLine().toLowerCase().split("\\s*,\\s*");
			/*
			 * read the next line which contains the first row of data. We are reading this
			 * line so that we can determine the data types of all the fields. Please note
			 * that ipl.csv file contains null value in the last column. If you do not
			 * consider this while splitting, this might cause exceptions later
			 */
			firstRowValues = reader.readLine().toLowerCase().split("\\s*,\\s*", -1);
		} catch (IOException e) {
			System.out.println("Reading from file failed");
		} catch (NullPointerException e) {

		}
		try {
			for (int colIndex = 0; colIndex < headers.length; colIndex++) {
				/*
				 * populate the header Map object from the header array. header map is having
				 * data type <String,Integer> to contain the header and it's index.
				 */
				header.put(headers[colIndex], colIndex);

				/*
				 * We have read the first line of text already and kept it in an array. Now, we
				 * can populate the dataTypeDefinition Map object. dataTypeDefinition map is
				 * having data type <Integer,String> to contain the index of the field and it's
				 * data type. To find the dataType by the field value, we will use getDataType()
				 * method of DataTypeDefinitions class
				 */
				dataTypeDef.put(colIndex,
						DataTypeDefinitions.getDataType(firstRowValues[colIndex]).getClass().getName());
			}
		} catch (NullPointerException e) {

		}
		/*
		 * once we have the header and dataTypeDefinitions maps populated, we can start
		 * reading from the first line. We will read one line at a time, then check
		 * whether the field values satisfy the conditions mentioned in the query,if
		 * yes, then we will add it to the resultSet. Otherwise, we will continue to
		 * read the next line. We will continue this till we have read till the last
		 * line of the CSV file.
		 */
		try {
			boolean isSelected;
			/* reset the buffered reader so that it can start reading from the first line */
			reader = new BufferedReader(new FileReader(queryParameter.getFile()));
			/*
			 * skip the first line as it is already read earlier which contained the header
			 */
			reader.readLine();
			/* read one line at a time from the CSV file till we have any lines left */
			while ((rowValue = reader.readLine()) != null) {
				/*
				 * once we have read one line, we will split it into a String Array. This array
				 * will continue all the fields of the row. Please note that fields might
				 * contain spaces in between. Also, few fields might be empty.
				 */
				rowValues = rowValue.split("\\s*,\\s*", -1);
				isSelected = true;
				/*
				 * if there are where condition(s) in the query, test the row fields against
				 * those conditions to check whether the selected row satifies the conditions
				 */
				if (queryParameter.getRestrictions() != null) {
					int iteration = 0;
					/*
					 * from QueryParameter object, read one condition at a time and evaluate the
					 * same. For evaluating the conditions, we will use evaluateExpressions() method
					 * of Filter class. Please note that evaluation of expression will be done
					 * differently based on the data type of the field. In case the query is having
					 * multiple conditions, you need to evaluate the overall expression i.e. if we
					 * have OR operator between two conditions, then the row will be selected if any
					 * of the condition is satisfied. However, in case of AND operator, the row will
					 * be selected only if both of them are satisfied.
					 */
					for (Restriction restriction : queryParameter.getRestrictions()) {
						if (iteration == 0) {

							isSelected = filter.evaluateExpression(restriction.getCondition(),
									rowValues[header.get(restriction.getPropertyName())],
									restriction.getPropertyValue(),
									dataTypeDef.get(header.get(restriction.getPropertyName())));
							iteration++;
							continue;
						}

						/*
						 * check for multiple conditions in where clause for eg: where salary>20000 and
						 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
						 */

						if (queryParameter.getLogicalOperators().get(iteration - 1) != null) {
							if (queryParameter.getLogicalOperators().get(iteration - 1).equalsIgnoreCase("and")) {
								isSelected = isSelected && filter.evaluateExpression(restriction.getCondition(),
										rowValues[header.get(restriction.getPropertyName())],
										restriction.getPropertyValue(),
										dataTypeDef.get(header.get(restriction.getPropertyName())));
							} else if (queryParameter.getLogicalOperators().get(iteration - 1).equalsIgnoreCase("or")) {
								isSelected = isSelected || filter.evaluateExpression(restriction.getCondition(),
										rowValues[header.get(restriction.getPropertyName())],
										restriction.getPropertyValue(),
										dataTypeDef.get(header.get(restriction.getPropertyName())));
							}
						}

					}
				}
				/*
				 * if the overall condition expression evaluates to true, then we need to check
				 * if all columns are to be selected(select *) or few columns are to be
				 * selected(select col1,col2). In either of the cases, we will have to populate
				 * the row map object. Row Map object is having type <Integer,String> to contain
				 * field Index and field value for the selected fields. Once the row object is
				 * populated, add it to DataSet Map Object. DataSet Map object is having type
				 * <Long,Row> to hold the rowId (to be manually generated by incrementing a Long
				 * variable) and it's corresponding Row Object.
				 */
				if (isSelected) {
					row = new Row();
					List<String> selectColumns = queryParameter.getFields();
					for (String selectColumn : selectColumns) {
						// check if all columns are required
						if (selectColumn.equals("*")) {

							for (int i = 0; i < rowValues.length; i++)
								row.put(headers[i], rowValues[i]);
							break;

						} else {
							row.put(headers[header.get(selectColumn)], rowValues[header.get(selectColumn)]);
						}
					}

					dataSet.put(rowid, row);

					rowid++;
				}
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			
		}
		/* return dataset object */
		return dataSet;
	}

}
