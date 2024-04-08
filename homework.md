Bigdata Assignment 2 Questions
Exploring User Interaction Data: PySpark vs. Pandas in Local Mode

Objective
This assignment aims to teach students the practical applications of PySpark and Pandas in processing and analyzing data in a local environment. By comparing these two popular Python libraries, students will gain insights into the strengths and efficiency of each tool, especially in a local Spark setup using threading mode.


Sample DataFrame
Consider a user interaction dataset with the following structure (this is just a demo example).: 
UserID	UserName	WatchedMovie	MovieGenre	SessionLength	LastLoginDate
1	ABC1	True	Comedy	100	2022-09-01
2	XY2	True	Drama	90	2023-08-15
3	ABC2	False	Action	80	2022-05-15

DataFrame Schema

The schema of the DataFrame based on the sample data . 

Column Name	DataType	Description	PySpark Type()
UserID	Integer	Unique identifier for each user	IntegerType
UserName	String	Name of the user	StringType
WatchedMovie	Boolean	Indicates if a movie was watched or not	BooleanType
MovieGenre	String	Genre of the watched movie	StringType
SessionLength	Integer	Duration of the session in minutes	IntegerType
LastLoginDate	Date	Date of the user's last login	DateType

Data Generation
To create a realistic and large dataset for testing queries in big data analysis. The goal is to ensure the dataset has a variety of values to effectively challenge and evaluate different query methods.
Methodology:
Utilize a simple Python script to generate the data. This approach is efficient and commonly used in cases where an existing dataset is not available or does not meet the requirements.
Ensure the generated data includes a wide range of values for thorough testing of different query scenarios.
Instructions for Data Generation:
Write a Python script using data generation libraries like Faker to create realistic data values.
The dataset should contain a minimum of 10,000 records. However, a larger dataset, such as one with 200,000 records or more, is recommended for a more accurate analysis of performance in big data contexts.
The script should be flexible, capable of generating any number of records as required.
Requirements for the Dataset:
The dataset should mimic a real-world structure and include various data types, including numerical, categorical, boolean, and date types.
Make sure the data encompasses a broad spectrum of values for each column to ensure robust testing of data queries.
To generate realistic and varied data for testing queries, it's advisable to utilize the Python Faker library.
This tool is straightforward and purpose-built for creating mock data. When generating your dataset, ensure it includes a wide range of values to effectively test different scenarios. 


Tasks
Task 1: Data Overview and Structure Analysis
Goal: Load the dataset and understand its structure, including data types and nullability.
Expected Outcome: Familiarity with data loading procedures and basic DataFrame inspection in both frameworks. Gain an understanding of the dataset's structure.
Task 2: Basic Data Aggregation
Goal: Calculate the total number of watched movies for each genre.
Expected Outcome: Perform simple aggregations and understand how to execute these operations in both PySpark and Pandas.

Task 3: Date-Based Insights
Goal: Identify the number of unique users who logged in during the last three months.
Expected Outcome: Handle date operations and derive insights based on recent user activity.

Task 4: User Behavior Analysis
Goal: Determine the average session length for users who have watched more than two movies.
Expected Outcome: Learn to combine conditional logic with aggregation to analyze specific user behaviors.

Task 5: Data Enhancement
Goal: Add a new column indicating the days since the last login for each user.
Expected Outcome: Experience with adding computed columns and manipulating date fields.

Submission Guidelines
Your code should be well-organized and commented, detailing each operation.
Execute the tasks in both PySpark and Pandas, comparing their performances in a local Spark setup.
Place them all in one file and organize them in a way to print the outcome for each in a nice way. 
Submit your work in a .py file format (only), ensuring it's correct executable.
Submit the dataset created and the Python code you wrote for data generation. 

Evaluation Criteria
Accuracy and completeness in performing the tasks.
Efficiency in code and performance comparison between PySpark and Pandas.
Good code and documentation - well organized
When submitting, please place all documents in a zip file and name as yourLastName_Assignment2.zip

Learning Objective

Data Generation and Handling:
Learn to generate large datasets programmatically.
Understand big data challenges, like memory constraints and processing time.
Performance Analysis:
Explore PySpark and Pandas' efficiency with large datasets.
Appreciate distributed computing in big data analytics.
Realistic Data Simulation:
Develop skills in creating realistic datasets for testing.
Understand how data variety impacts processing.
Schema and Data Types:
Deepen knowledge of data types and schema definition.
Enhance skills in handling various data types, including dates and booleans.
Query Optimization:
Identify efficient query-writing techniques for large datasets.Comprehend the performance impact of different data operations.