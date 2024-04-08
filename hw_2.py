# %%
# Pandas Task 1
URL = 'hw_2_user_data.csv'
COLS = ['UserID','UserName','WatchedMovie','MovieGenre','SessionLength','LastLoginDate']
MAX_ROWS = 50
import pandas as pd

def printPd(dataframe, num_rows = MAX_ROWS):
    # nicely prints given number of rows from the dataframe.
    total = len(dataframe)
    print(dataframe.head(num_rows).to_markdown())
    print(f'Showing first {min(total, num_rows)} rows out of {total}\n')

def printSection(section_title):
    print(f'\n**********\n**********> {section_title} \n**********')

pa_df = pd.read_csv(URL, parse_dates=['LastLoginDate'])

# The fields 'UserName' and 'MovieGenre' were parsed as type 'object' by pandas.
# In order to use the properly, we will convert these types to python string.
pa_df['UserName'] = pa_df['UserName'].astype('string')
pa_df['MovieGenre'] = pa_df['MovieGenre'].astype('string')
# pa_df

printSection('Pandas Task 1 - Load Data')

print(f'DATA TYPES: \n------------\n{pa_df.dtypes}')
print(f'\nDATA: \n------------\n')
printPd(pa_df)

print('Pandas takes about 0.6 second to load the data (Task 1) (tested on Jupyter Notebook).')

# %%
# PySpark Task 1
from pyspark.sql.functions import to_date, months_between, current_date, datediff, col
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, BooleanType

def printSprk(dataframe, num_rows=MAX_ROWS):
    total = dataframe.count()
    dataframe.show(num_rows, truncate = False)
    print(f'Total # of rows = {total}\n')

spark = SparkSession.builder.getOrCreate()

# Ignore all logs except errors when running this script on pyspark (i.e. spark-submit)
spark.sparkContext.setLogLevel('ERROR')

# Task 1
# Read json dataset into a DataFrame and apply the schema defined above.
df = spark.read.csv('hw_2_user_data.csv', header=True) 

# Rename columns and update data types where needed. 
df = df.withColumnRenamed('_c0', 'Index')\
       .withColumn('UserID', df['UserID'].cast(IntegerType()))\
       .withColumn('SessionLength', df['SessionLength'].cast(IntegerType()))\
       .withColumn('WatchedMovie', df['WatchedMovie'].cast(BooleanType()))\
       .withColumn('LastLoginDate', to_date(df['LastLoginDate']))

printSection('PySpark Task 1 - Load Data')

# Display the updated schema 
df.printSchema()
# Display data
printSprk(df)

print('PySpark also takes about 5.3 seconds to load the data (Task 1) (tested on Jupyter Notebook).')
print('PySpark performs significantly worse than Pandas to load the data.')

# %%
# Pandas Task 2
# movie_genres = df['MovieGenre'].unique().dropna()

printSection('Pandas Task 2 - # of Watched Moves by Genre')

# First we find rows where a movie was watched, 
# then group this data by genre 
# and then get the count for each genre.
r = pa_df[pa_df['WatchedMovie'] == True]\
    .groupby(['MovieGenre'])\
    .agg({'WatchedMovie': ['count']})

printPd(r)

print('Pandas takes about 0 second to query # of Watched Moves by Genre (Task 2) (tested on Jupyter Notebook).')

# %%
# PySpark Task 2
# Calculate the total number of watched movies for each genre.

printSection('PySpark Task 2 - # of Watched Moves by Genre')

# Here we do the same as with pandas, but additionlly we sort by movie genre 
# in order to match results with pandas.
r = df.filter(df.WatchedMovie == True).groupBy('MovieGenre').count()\
       .orderBy('MovieGenre')

printSprk(r)
print('PySpark however takes about 1.0 seconds to query # of Watched Moves by Genre (Task 2) (tested on Jupyter Notebook).')
print('Which means Pandas performs better on Task 2.')

# %%
# Pandas Task 3
import numpy as np

printSection('Pandas Task 3 - # of Users in Last 3 Months')

# Find records where today - LastLoginDate is ~3 months. 
# For this we will find the time delta and devide by 30 day period to get the difference in months.
# Then we can filter rows out where difference is larger than 3.

r = len(pa_df[((pd.to_datetime('today') - pa_df['LastLoginDate']) / np.timedelta64(1, 'D')) < 92]\
    ['UserID'].unique())

print(r)
# Note that we're using unit 'D' instead of unit 'M' as 'M' is no longer supported by numpy. 
# And we're using # of days so a 3 month period comes around to ~92

print('Pandas takes about 0 second to query # of Users in Last 3 Months (Task 3) (tested on Jupyter Notebook).')

# %%
# PySpark Task 3
# Identify the number of unique users who logged in during the last three months.

printSection('PySpark Task 3 - # of Users in Last 3 Months')

# Life is simple when there's months_between to find users who logged in within last 3 months. 
r = df.filter(months_between(current_date(), df.LastLoginDate) <= 3)\
       .select('UserID').distinct().count()

print(r)

print('PySpark takes about 0.8 second to query # of Users in Last 3 Months (Task 3) (tested on Jupyter Notebook).')
print('So Pandas performs better on Task 3.')

# %%
# Pandas Task 4

printSection('Pandas Task 4 - Avg Session length for avid movie watchers')

# Filter by watched movies and group by UserID while aggregating count for 
# watched movies and avg session length. Then filter results where watched count > 2.

r = pa_df[pa_df['WatchedMovie'] == True].groupby(['UserID'])\
    .agg({'WatchedMovie': ['count'], 'SessionLength': ['mean']})
r = r[r['WatchedMovie']['count'] > 2]

printPd(r)

print('Pandas takes about 0 second to query Avg Session length for avid movie watchers (Task 4) (tested on Jupyter Notebook).')

# %%
# PySpark Task 4
# Determine the average session length for users who have watched more than two movies.

printSection('PySpark Task 4 - Avg Session length for avid movie watchers')

# Filter by watched movies and group by UserID while aggregating count for 
# watched movies and avg session length. Then filter results where watched count > 2.
# Again we sort by user id to match result with pandas.
r = df.filter(df.WatchedMovie == True)\
    .groupBy('UserID').agg({'WatchedMovie': 'count', 'SessionLength': 'mean'})\
    .where(col('count(WatchedMovie)') > 2).orderBy('UserID')

printSprk(r)

print('PySpark takes about 0.6 second to query Avg Session length for avid movie watchers (Task 4) (tested on Jupyter Notebook).')
print('Here, Pandas performs better on Task 4.')

# %%
# Pandas Task 5

printSection('Pandas Task 5 - # of Days Since Last Login')

# Insert new column by computing time delta (#days) between today and LastLoginDate.
pa_df.insert(7, 'DaysSinceLastLogin', ((pd.to_datetime('today') - pa_df['LastLoginDate']) / np.timedelta64(1, 'D')))

# Cast DaysSinceLastLogin to int for better readability.
pa_df['DaysSinceLastLogin'] = pa_df['DaysSinceLastLogin'].astype(int)

printPd(pa_df)

print('Pandas takes about 0 second to compute # of Days Since Last Login and add as new column (Task 5) (tested on Jupyter Notebook).')

# %%
# PySpark Task 5
# Add a new column indicating the days since the last login for each user.

printSection('PySpark Task 5 - # of Days Since Last Login')

# Set new column value using datediff, easy :)
df = df.withColumn('DaysSinceLastLogin', datediff(current_date(), df.LastLoginDate)) 
# df.printSchema()
printSprk(df)

print('PySpark takes about 0.1 second to compute # of Days Since Last Login and add as new column (Task 5) (tested on Jupyter Notebook).')
print('So Pandas performs faster on Task 5.')

print('\nOverall, we can say that for this dataset, Pandas performs better than PySpark.')
