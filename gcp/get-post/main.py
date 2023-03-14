
import mysql.connector

'''
Remplacer user par votre nom d'utilisateur, ici zgobji
'''

def main_function(request):
    # Connect to Cloud SQL
    conn = mysql.connector.connect(user='zgobji', host='host', 
                              database='bdd_zied_demo')
    
    cursor = conn.cursor()

    if request.method == 'POST':
        data = request.get_json()

        for i in range(len(data['name'])):
            query = "INSERT INTO table_etoiles (name, distance, stellar_magnitude, planet_type) VALUES ('{}', '{}', '{}', '{}')".format(
                data['name'][i], data['distance'][i], data['stellar_magnitude'][i], data['planet_type'][i]
            )
            cursor.execute(query)
            conn.commit()
                
        return 'Text stored successfully in Cloud SQL.'
    
    elif request.method == 'GET':

        # Retrieve all data from Cloud SQL
        query = "SELECT * FROM table_etoiles"
        cursor.execute(query)
        result = cursor.fetchall()
        
        # Format the result
        result_str = '\n'.join([str(row) for row in result])
        
        return result_str


