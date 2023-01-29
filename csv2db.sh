# This script
# Extracts data from /etc/passwd file into a CSV file.

# The csv data file contains the user name, user id and 
# home directory of each user account defined in /etc/passwd

# Transforms the text delimiter from ":" to ",".
# Loads the data from the CSV file into a table in PostgreSQL database.

#Extract phase

echo "Extracting Data"
#Extract the columns 1 (User name), 2(User id) and #6(home directory path) frpm /etc/passwd

cut -d":" -f1,3,6 /etc/passwd

#Redirecting the extracted data to a file named 'extracted-data.txt'
cut -d":" -f1,3,6 /etc/passwd
cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt

#Transforming phase
echo "Transforming data"

#Read the extracted data and replace colons with commas

tr ":" ","<extracted-data.txt

#Create a new csv filed titled 'transformed-data.csv' to load the transformed data
tr ":" ","<extracted-data.txt>transformed-data.csv
# Load phase
echo "Loading data"
# Send the instructions to connect to 'template1' and
# copy the file to the table 'users' through command pipeline.

echo "\c template1;\COPY users  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV;" | psql --username=postgres --host=localhost