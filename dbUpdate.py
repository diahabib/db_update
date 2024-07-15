import mysql.connector

def execute_query():
    try:
        # Connexion à la base de données BASE1 MySQL
        conn1 = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="BASE1"
        )
        cur1 = conn1.cursor()

        # Connexion à la base de données BASE2 MySQL
        conn2 = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="BASE2"
        )
        cur2 = conn2.cursor()

        # Connexion à la base de données CC MySQL
        conn3 = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="CC"
        )
        cur3 = conn3.cursor()

        # Requête SQL pour récupérer les données de BASE1 et BASE2
        select_query = """
        SELECT t1.C2T1, t2.C1T2
        FROM BASE1.T1 t1
        JOIN BASE2.T2 t2 ON t1.C1T1 = t2.C1T2
        """

        # Exécuter la requête pour récupérer les données
        cur1.execute(select_query)
        results = cur1.fetchall()

        # Requête SQL pour insérer dans T3 de CC
        insert_query = """
        INSERT INTO T3 (C2T1, C1T2)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
        C2T1 = VALUES(C2T1), C1T2 = VALUES(C1T2)    
        """

        # Exécuter les insertions dans CC
        cur3.executemany(insert_query, results)
        conn3.commit()

        # Fermeture des connexions
        cur1.close()
        conn1.close()
        cur2.close()
        conn2.close()
        cur3.close()
        conn3.close()

        print("Requête exécutée avec succès.")

    except mysql.connector.Error as e:
        print(f"Erreur lors de l'exécution de la requête : {e}")

if __name__ == "__main__":
    execute_query()
