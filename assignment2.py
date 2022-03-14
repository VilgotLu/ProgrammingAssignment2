import mysql.connector
from mysql.connector import errorcode
import csv

cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1')
DB_NAME = 'vl222kz_pa2'
cursor = cnx.cursor()


# Function to create the database
def createDatabase(cursor, DB_NAME):
    try:
        cursor.execute('CREATE DATABASE {} DEFAULT CHARACTER SET \'utf8\''.format(DB_NAME))
    except mysql.connector.Error as err:
        print('Failed to create database {}'.format(err))
        exit(1)


# Function to create a table or view
def createTable(cursor, createStatement, tableName):
    try:
        print("Creating table {}: ".format(tableName))
        cursor.execute(createStatement)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


# Parses csv files and returns a list of tuples
def parse(filestring):
    tupleList = []
    with open(filestring) as file:
        csvreader = csv.reader(file, delimiter=';')
        # Skips the first row because it only contains the column names
        for row in list(csvreader)[1:]:
            for i in range(len(row)):
                # N/A values should be NULL
                if row[i] == 'N/A':
                    row[i] = None
            tupleList.append(tuple(row))
        return tupleList


# Function to insert tuples to a table
def insertInto(cursor, tuplelist, insertStatement, tableName):
    try:
        # Uses executemany() to insert all the tuples in the list using the insertStatement
        print('Inserting values into {}:'.format(tableName))
        cursor.executemany(insertStatement, tuplelist)
    except mysql.connector.Error as err:
        print(err.msg)
    else:
        # Commits the changes and prints OK if successful
        cnx.commit()
        print('OK')


# Function to print the main menu
def showMainMenu():
    print()
    print('1. What\'s the percentage of people in a country living in a given capital?')
    print('2. List all countries in Europe by their number of official languages.')
    print('3. For each language family, what\'s the average population of the countries ')
    print('   that have an official language from that language family?')
    print('4. Which developed countries have a capital that start with a given letter')
    print('5. What are the 5 countries that are closest in size to the given country?')
    print('Q. Quit')
    print('------------------------------------------------------------------------------')


# SQL statement to create the countries table
createCountries = '''CREATE TABLE countries (name VARCHAR(50) NOT NULL, 
                       population INT, 
                       area INT, 
                       continent VARCHAR(20), 
                       GDP INT, 
                       HDI DECIMAL(4,3), 
                       PRIMARY KEY(name))'''

# Countries insert statement
insertCountries = 'INSERT INTO countries VALUES (%s, %s, %s, %s, %s, %s)'

# SQL statement to create the capitals table
createCapitals = '''CREATE TABLE capitals (name VARCHAR(50) NOT NULL, 
                       country VARCHAR(50) NOT NULL, 
                       population INT, 
                       PRIMARY KEY(name), 
                       FOREIGN KEY (country) REFERENCES countries(name))'''

# Capitals insert statement
insertCapitals = 'INSERT INTO capitals VALUES (%s, %s, %s)'

# SQL statement to create the languages table
createLanguages = '''CREATE TABLE languages (name VARCHAR(40) NOT NULL, 
                       family VARCHAR(40), 
                       native_speakers INT, 
                       PRIMARY KEY (name))'''

# languages insert statement
insertLanguages = 'INSERT INTO languages VALUES (%s, %s, %s)'

# SQL statement to create the language_countries table
createLangCountries = '''CREATE TABLE language_countries (country VARCHAR(50) NOT NULL, 
                       language VARCHAR(50) NOT NULL,  
                       PRIMARY KEY (country, language), 
                       FOREIGN KEY (country) REFERENCES countries(name), 
                       FOREIGN KEY (language) REFERENCES languages(name))'''

# Language_countries insert statement
insertLangCountries = 'INSERT INTO language_countries VALUES (%s, %s)'

# SQL statement to create the population_stats view
createPopulationStats = '''CREATE VIEW population_stats AS 
                        SELECT capitals.name AS capital, capitals.population AS capital_population, 
                        countries.name AS country, countries.population AS 'country_population' 
                        FROM capitals JOIN countries ON capitals.country = countries.name'''


try:
    # Tries to connect to the database
    cursor.execute('USE {}'.format(DB_NAME))
except mysql.connector.Error as err:
    # If the database doesn't exist:
    # Creates the database, connects to it, creates tables and views, and inserts the data to the tables.
    print('Database {} does not exist'.format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        createDatabase(cursor, DB_NAME)
        print('Database {} created successfully'.format(DB_NAME))
        cnx.database = DB_NAME
        createTable(cursor, createCountries, 'countries')
        insertInto(cursor, parse('countryfile.csv'), insertCountries, 'countries')
        createTable(cursor, createCapitals, 'capitals')
        insertInto(cursor, parse('capitalfile.csv'), insertCapitals, 'capitals')
        createTable(cursor, createLanguages, 'languages')
        insertInto(cursor, parse('languages.csv'), insertLanguages, 'languages')
        createTable(cursor, createLangCountries, 'language_countries')
        insertInto(cursor, parse('country_languages.csv'), insertLangCountries, 'language_countries')
        createTable(cursor, createPopulationStats, 'population_stats')
    else:
        print(err)


showMainMenu()
choice = input('Choose a query: ')

# Quits if the user types Q in the main menu.
while choice.lower() != 'q':
    # Query #1
    if choice == '1':
        capitalChoice = input('\nChoose a capital: ')
        cursor.execute('''SELECT capital, country, capital_population / country_population * 100 AS percentage
                        FROM population_stats  
                        WHERE capital = \'{}\''''.format(capitalChoice))
        result = cursor.fetchall()
        rowcount = cursor.rowcount

        if rowcount == 0:
            print('\nCouldn\'t find capital \'{}\''.format(capitalChoice))
        else:
            for x in result:
                print('\nThe percentage of people in {} that live in {} is {}%'.format(x[1], x[0], x[2]))
        
        press = input('\nPress enter to return to the main menu: ')
    # Query #2
    elif choice == '2':
        cursor.execute('''SELECT countries.name AS country, COUNT(language_countries.language) AS language_count
                        FROM countries JOIN language_countries ON countries.name = language_countries.country
                        WHERE countries.continent = 'Europe'
                        GROUP BY country  
                        ORDER BY `language_count`  DESC''')
        
        print('\nCountry                 | languages\n-------------------------------------')
        for x in cursor:
            print('{:<23} | {}'.format(x[0], x[1]))
            print('-------------------------------------')

        press = input('\nPress enter to return to the main menu: ')
    # Query #3
    elif choice == '3':
        cursor.execute('''SELECT languages.family AS language_family, AVG(countries.population) AS average_population
                        FROM countries JOIN language_countries ON countries.name = language_countries.country
                        JOIN languages ON language_countries.language = languages.name
                        GROUP BY languages.family  
                        ORDER BY average_population DESC''')

        print('\nlanguage family | average population\n----------------------------------')
        for x in cursor:
            print('{:<15} | {}'.format(x[0], x[1]))
            print('----------------------------------')

        press = input('\nPress enter to return to the main menu: ')
    # Query #4
    elif choice == '4':
        letterChoice = input('\nChoose a letter: ')
        cursor.execute('''SELECT countries.name, capitals.name
                        FROM countries JOIN capitals ON countries.name = capitals.country
                        WHERE countries.HDI >= 0.75 
                        AND capitals.name LIKE \'{}%\''''.format(letterChoice))
        result = cursor.fetchall()
        rowcount = cursor.rowcount

        if rowcount == 0:
            print('\nCouldn\'t find any developed countries with capitals starting with {}.'.format(letterChoice))
        else:
            print('\ncountry                        | capital\n----------------------------------------------------')
            for x in result:
                print('{:<30} | {}'.format(x[0], x[1]))
                print('----------------------------------------------------')
        press = input('\nPress enter to return to the main menu: ')
    # Query #5
    elif choice == '5':
        countryChoice = input('\nChoose a country: ')
        cursor.execute('''SELECT compareCountries.name, ABS(compareCountries.area-chosenCountry.area) AS difference, 
                        ABS(compareCountries.area-chosenCountry.area) / chosenCountry.area * 100 AS percentage 
                        FROM countries compareCountries, countries chosenCountry 
                        WHERE chosenCountry.name = \'{}\' AND compareCountries.name != \'{}\'  
                        ORDER BY `difference`  ASC
                        LIMIT 5'''.format(countryChoice, countryChoice))
        result = cursor.fetchall()
        rowcount = cursor.rowcount

        if rowcount == 0:
            print('\nCouldn\'t find country \'{}\''.format(countryChoice))
        else:
            print('\n# | Country                         | Difference (km2) | Percentage difference')
            print('----------------------------------------------------------------------------------')
            i = 1
            for x in result:
                print('{:<1} | {:<31} | {:<16} | {}%'.format(i, x[0], x[1], x[2]))
                print('----------------------------------------------------------------------------------')
                i += 1

        press = input('\nPress enter to return to the main menu: ')
    # Invalid input
    else:
        print('Invalid input')
    showMainMenu()
    choice = input('Choose a query: ')

# Closes the cursor and the connection
cursor.close()
cnx.close()
