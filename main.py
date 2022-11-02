import sqlite3
import json
import requests


def safe_get_request(url: str):
    """This function makes a GET request to the URL passed in as its single parameter.
    If an exception is thrown while trying to execute the GET request, 'None' is returned in place of a response object.
    If the GET request is successful , a response object is returned."""

    response = None

    # if requests.get() throws an exception, the 'response' variable will remain as 'None'

    try:
        response = requests.get(url)
        print(f'GET request executed with no errors. Response object created:\n'
              f'Response object: <{hex(id(response))}>\n')
    except requests.exceptions.RequestException as request_exception:
        print_red_text(f'GET request FAILED with the following error: {request_exception}\n')
    finally:
        return response
        # if requests.get() throws an exception, the 'response' variable will remain as 'None'


def print_red_text(text_str: str):
    """ Prints the incoming string parameter as RED text to the terminal """
    print(f'\033[91m {text_str}\033[00m')


def issue_get_request(target_url: str):
    """ This function issues a GET request to the target URL passed in as a single parameter
    a response object is returned
    The status code of the request is also reported"""

    # safe get request returns none if an exception happens while executing ita function body
    response_obj = safe_get_request(target_url)
    if response_obj is None:
        print_red_text(f'A GET request error has occurred: No response object!\n')
        return None

    if response_obj.status_code != 200:
        print_red_text(f'The GET request was NOT successful\n{response_obj.status_code} [{response_obj.reason}]\n')
        return response_obj
    else:
        print(f'The GET request was successful\n{response_obj.status_code} [{response_obj.reason}]\n')
        return response_obj


def convert_content_to_json(response_obj: requests.Response):
    json_data_obj = None
    if response_obj is None:
        print_red_text(f'JSON decoder not executed: No response object! \n')
        return None
    try:
        json_data_obj = response_obj.json()
        print(f'Response object content converted to JSON object.\n')
    except requests.exceptions.JSONDecodeError as json_decode_error:
        print_red_text(f'An error occurred while tring to convert tje response content to a JSON object: '
                       f'{json_decode_error}\n')
    finally:
        return json_data_obj


def establish_database_connection(database_name: str):
    """This function tries to connect to a database with the name according to the incoming string.
     A database connection is returned if successful.
    None is returned if a connection was unable be established"""

    db_connection = None
    try:
        db_connection = sqlite3.connect(database_name)
        print(f'Connection to database \'{database_name}\' was established\n'
              f'database connection: {db_connection}\n')
    except sqlite3.Error as db_error:
        print_red_text(f'An error occurred while trying to connect to database{database_name}:'
                       f'{db_error}')
    finally:
        return db_connection


def create_db_cursor(db_connection_obj: sqlite3.Connection):
    """This function creates a sqlite3 cursor object on the database connection incoming as its single parameter
     'None' is returned if the cursor object could not be created"""

    cursor_obj = None
    if db_connection_obj is None:
        print_red_text(f' Cursor object NOT created: No response object! \n')
        return None
    try:
        cursor_obj = db_connection_obj.cursor()
        print(f'Cursor object created on {db_connection_obj}\n'
              f'Cursor object: {cursor_obj}')
    except sqlite3.Error as db_error:
        print_red_text(f'A cursor object could not be created on database -connection {db_connection_obj}\n'
                       f'{db_error}')
    finally:
        return cursor_obj


def create_a_table(db_cursor: sqlite3.Cursor):
    """This function cretaes a table in the meteoriteLandings databse and loops through the different values"""
    json_data_object = None
    if db_cursor is None:
        print_red_text(f'Table not created: No cursor object\n')
        return None
    try:

        db_cursor.execute('''CREATE TABLE IF NOT EXISTS meteoriteLandings(
                                            Africa_MiddleEast_Meteorites TEXT,
                                            Europe_Meteorites TEXT,
                                            Upper_Asia_Meteorites TEXT,
                                            Lower_Asia_Meteorites TEXT,
                                            Australia_Meteorites TEXT,
                                            North_America_Meteorites TEXT,
                                            South_America_Meteorites TEXT,);’‘’

        ''')
        db_cursor.execute('''DELETE FROM meteoriteLandings''')

        for record in json_data_object:
            db_cursor.execute('''INSERT INTO meteoriteLandings VALUES(?, ?, ?, ?)''',
                              (record.get('name', None),
                               record.get('mass', None),
                               record.get('reclat', None),
                               record.get('reclong', None)))
            db_cursor.execute('SELECT * FROM meteoriteLandings')


    except sqlite3.Error as db_error:
        print_red_text(f'Table could not be created on database \n'
                       f'{db_error}')
    finally:
        return db_cursor


def main():
    target_url_str = 'https://data.nasa.gov/resource/gh4g-9sfh.json'
    response_obj = issue_get_request(target_url_str)

    db_connection = establish_database_connection('meteoriteLandings.db')

    db_cursor = create_db_cursor(db_connection)

    json_data_object = convert_content_to_json(response_obj)

    db_table = create_a_table(db_cursor)

    print(json_data_object)


if __name__ == '__main__':
    main()
