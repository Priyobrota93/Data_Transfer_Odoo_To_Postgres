from flask import Flask, request, jsonify
from flask_httpauth import HTTPTokenAuth
import xmlrpc.client
import psycopg2

app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')

# Replace with your API secret key
API_SECRET_KEY = 'clarexITpriyo'

tokens = {
    API_SECRET_KEY: "user"
}

@auth.verify_token
def verify_token(token):
    if token in tokens:
        return tokens[token]
    return None

 
ODOO_URL = 'http://localhost:8089/'
ODOO_DB = 'new_db'
ODOO_USERNAME = 'admin1'
ODOO_PASSWORD = 'admin1'

 
PG_HOST = 'localhost'
PG_DB = 'etl'
PG_USER = 'postgres'
PG_PASSWORD = 'openpgpwd'

def get_user_id_by_name(models, uid, user_name):
    try:
        user_ids = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.users', 'search', [[['name', '=', user_name]]])
        if user_ids:
            print(user_ids)
            return user_ids[0]
        else:
            return None
    except Exception as e:
        print("Error retrieving user ID from Odoo:", e)
        return None

def get_employee_id_by_name(models, uid, employee_name):
    try:
        employee_ids = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'hr.employee', 'search', [[['name', '=', employee_name]]])
        if employee_ids:
            print(employee_ids)
            return employee_ids[0]
        else:
            return None
    except Exception as e:
        print("Error retrieving employee ID from Odoo:", e)
        return None

@app.route('/transfer_data', methods=['POST'])
@auth.login_required
def transfer_data():
    try:
        # Connect to Odoo
        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(ODOO_URL))
        uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})

        if not uid:
            return jsonify({"error": "Unable to authenticate with Odoo"}), 401

        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(ODOO_URL))

        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        pg_cursor = pg_conn.cursor()
        print("Connected to PostgreSQL database successfully")

        # Fetch data from Odoo's hr_test_portal_access table
        records = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'hr_test_portal_access', 'search_read', 
                                     [[]], {'fields': ['id', 'create_uid', 'write_uid', 'create_date', 'write_date','department_id','job_id',
                                                       'name', 'job_title','work_phone','work_email','employee_id'],
                                            'limit': 1000})  # adjust limit as per your data volume
        
        # Insert records into PostgreSQL
        for record in records:
            create_uid = get_user_id_by_name(models, uid, record.get('create_uid')[1]) if record.get('create_uid') else 0
            write_uid = get_user_id_by_name(models, uid, record.get('write_uid')[1]) if record.get('write_uid') else 0
            employee_id = get_employee_id_by_name(models, uid, record.get('employee_id')[1]) if record.get('employee_id') else 0

            create_uid = create_uid if create_uid is not None else 0
            write_uid = write_uid if write_uid is not None else 0
            employee_id = employee_id if employee_id is not None else 0
            
            record['create_uid'] = create_uid
            record['write_uid'] = write_uid
            record['employee_id'] = employee_id
            
            # Convert False to None or 0 for integer fields
            record['department_id'] = record['department_id'] if record['department_id'] is not False else None
            record['job_id'] = record['job_id'] if record['job_id'] is not False else None
            
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['%s'] * len(record))
            values = [None if v is False else v for v in record.values()]
            
            pg_cursor.execute("INSERT INTO hr_test_portal_access ({}) VALUES ({})".format(columns, placeholders), values)

        pg_conn.commit()
        print("Data transfer completed successfully")
        
        return jsonify({"message": "Data transfer completed successfully"}), 200
        
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed")


@app.route('/view_data', methods=['GET'])
@auth.login_required
def view_data():
    try:
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        pg_cursor = pg_conn.cursor()
        print("Connected to PostgreSQL database successfully")

        # Retrieve data from PostgreSQL
        pg_cursor.execute("SELECT * FROM hr_test_portal_access")
        records = pg_cursor.fetchall()
        columns = [desc[0] for desc in pg_cursor.description]

        result = []
        for row in records:
            result.append(dict(zip(columns, row)))

        return jsonify(result), 200

    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed")

if __name__ == '__main__':
    app.run(debug=True)
