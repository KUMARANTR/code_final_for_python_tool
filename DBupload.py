import pymysql
import ssl
import Shared

from helper_functions import get_asset_path

# SSL certificate paths (update these paths as needed)
sslca = get_asset_path('server-ca.pem')
sslkey = get_asset_path('client-key.pem')
sslcert = get_asset_path('client-cert.pem')


def EntryDB_inbound():
    connection = pymysql.connect(
        host='10.216.252.8',
        user=Shared.userid,
        passwd=Shared.password,
        db='client_data',
        port=3306,
        ssl_ca=sslca,
        ssl_key=sslkey,
        ssl_cert=sslcert
    )

    cursor = connection.cursor()
    query = "SHOW TABLES;"
    cursor.execute(query)

    tables = cursor.fetchall()
    # Filter inbound tables based on naming convention
    inbound_tables = [table[0] for table in tables if "inbound" in table[0].lower()]

    cursor.close()
    connection.close()

    return inbound_tables


def EntryDB_outbound():
    connection = pymysql.connect(
        host='10.216.252.8',
        user=Shared.userid,
        passwd=Shared.password,
        db='client_data',
        port=3306,
        ssl_ca=sslca,
        ssl_key=sslkey,
        ssl_cert=sslcert
    )

    cursor = connection.cursor()
    # Query to get all table names in the specified database
    query = "SHOW TABLES;"
    cursor.execute(query)

    # Fetch all table names
    tables = cursor.fetchall()
    all_tables = [table[0] for table in tables]  # Extract table names

    cursor.close()  # Close the cursor
    connection.close()  # Close the database connection

    return all_tables  # Return the list of all table names as strings


def test_db_connection(user, passwd):
    try:
        # Attempt to connect to the database
        connection = pymysql.connect(
            host='10.216.252.8',
            user=user,
            passwd=passwd,
            db='client_data',
            port=3306,
            ssl_ca=sslca,
            ssl_key=sslkey,
            ssl_cert=sslcert
        )
        connection.close()  # Close the connection
        return True  # Connection successful
    except pymysql.MySQLError as e:
        #print(f"Error: {e}")
        return False  # Connection failed
