from sqlalchemy import create_engine

mysql_conn = {
    "Host": "localhost",
    "Port": "3306",  # or 6603
    "User": "root",
    "Password": "pwd",
    "Database": "db"
}

for key in mysql_conn.keys():
    if mysql_conn[key] is None or len(mysql_conn[key]) == 0:
        print("Enter {}".format(key))
        mysql_conn[key] = input()

try:
    engine = create_engine(
        "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4".format(
            mysql_conn["User"],
            mysql_conn["Password"],
            mysql_conn["Host"],
            mysql_conn["Port"],
            mysql_conn["Database"],
        )
    )

except Exception as e:
    print("Conect to MYSQL failed, {}".format(e))
    exit()

with engine.connect() as con:
    res = con.execute("show databases;")
    print('databases', list(res))
    res = con.execute("show tables;")
    print('tables', list(res))
    res = con.execute("Select count(*) from db.car_garage;")
    print(list(res))

