import xmlrpc.client
import psycopg2
# Odoo connection parameters
ODOO_URL = 'http://localhost:8089/'
ODOO_DB = 'new_db'
ODOO_USERNAME = 'admin1'
ODOO_PASSWORD = 'admin1'

PG_HOST = 'localhost'
PG_DB = 'etl'
PG_USER = 'postgres'
PG_PASSWORD = 'openpgpwd'

common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(ODOO_URL))
uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})

models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(ODOO_URL))

def get_user_id_by_name(user_name):
    try:
        # Lookup user ID by name in Odoo
        user_ids = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.users', 'search', [[['name', '=', user_name]]])
        if user_ids:
            return user_ids[0]
        else:
            return None
    except Exception as e:
        print("Error retrieving user ID from Odoo:", e)
        return None

def get_employee_id_by_name(employee_name):
    try:
        employee_ids = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'hr.employee', 'search', [[['name', '=', employee_name]]])
        if employee_ids:
            return employee_ids[0]
        else:
            return None
    except Exception as e:
        print("Error retrieving employee ID from Odoo:", e)
        return None

if uid:
    try:
        pg_conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        pg_cursor = pg_conn.cursor()
        print("Connected to PostgreSQL database successfully")

    
        records = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'hr_test_portal_access', 'search_read', 
                                     [[]], {'fields': ['id', 'create_uid', 'write_uid', 'create_date', 'write_date','department_id','job_id',
                                                       'name', 'job_title','work_phone','work_email','employee_id'],
                                            'limit': 1000})  # adjust limit as per your data volume
        
     
        for record in records:
          
            create_uid = get_user_id_by_name(record.get('create_uid')[1]) if record.get('create_uid') else 0
            write_uid = get_user_id_by_name(record.get('write_uid')[1]) if record.get('write_uid') else 0
            employee_id = get_employee_id_by_name(record.get('employee_id')[1]) if record.get('employee_id') else 0
            
            create_uid = create_uid if create_uid is not None else 0
            write_uid = write_uid if write_uid is not None else 0
            employee_id = employee_id if employee_id is not None else 0
            
            record['department_id'] = record['department_id'] if record['department_id'] is not False else None
            record['job_id'] = record['job_id'] if record['job_id'] is not False else None
            
            record['create_uid'] = create_uid
            record['write_uid'] = write_uid
            record['employee_id'] = employee_id
            
            columns = ', '.join(record.keys())
            placeholders = ', '.join(['%s'] * len(record))
            values = [None if v is False else v for v in record.values()]
            
            pg_cursor.execute("INSERT INTO hr_test_portal_access ({}) VALUES ({})".format(columns, placeholders), values)

        pg_conn.commit()
        print("Data transfer completed successfully")
        
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
    finally:
        if pg_conn:
            pg_conn.close()
            print("PostgreSQL connection closed")
else:
    print("Unable to authenticate with Odoo")
