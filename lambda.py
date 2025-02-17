import json
import base64
import pymysql
# Database connection parameters
DB_HOST = "newdb.cp2g0eygu4ab.us-east-1.rds.amazonaws.com"
DB_USER = "admin"  # Replace with your RDS username
DB_PASSWORD = "********"  # Replace with your RDS password
DB_NAME = ""

def lambda_handler(event, context):
    # Connect to the MySQL database
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    
    try:
        with connection.cursor() as cursor:
            # Iterate over messages
            partition = list(event['records'].keys())[0]
            messages = event['records'][partition]

            for data in messages:
                # Decode and parse the message
                decoded_value = base64.b64decode(data['value']).decode('utf-8')
                value = json.loads(decoded_value)

                # Extract the 'name' field
                name = value.get('name')
                print(f"Name: {name}")

                # Insert the name into the database
                if name:
                    sql = "INSERT INTO kafkanames (name) VALUES (%s)"
                    cursor.execute(sql, (name,))
            
            # Commit the transaction
            connection.commit()
    
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {e}")
        }
    
    finally:
        connection.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps('Name(s) stored successfully in the database.')
    }
