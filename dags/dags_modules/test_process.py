from settings.config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
import psycopg2


def insert_match():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=5432
        )
        cur = conn.cursor()

        insert_query = """
            INSERT INTO atp_matches (tournament, "year", winner, loser, score, round, surface)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, ('test', 2025, 'test', 'test', 'test', 'test', 'test'))

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Match inserted successfully!")

    except Exception as e:
        print("❌ Error inserting match:", e)


if __name__ == '__main__':
    insert_match(None)
