import datetime
import pymysql

# mysql工具类
class MySQL(object):
    def __init__(self, host: str, user: str, password: str, database: str, port: int):
        """连接到MySQL数据库"""
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )

    def close(self):
        self.connection.close()


    def connect(self):
        self.connection.connect()

    def insert(self, table_name: str, fields: list, data: list):
        """批量插入"""
        self.connection.connect()
        cursor = self.connection.cursor()
        keystr = ""
        for i in range(0, len(fields)):
            if i < len(fields)-1:
                keystr = keystr+fields[i]+","
            else:
                keystr = keystr+fields[i]
        valstr = ""
        for i in range(0, len(data)):
            valstr = valstr+"("
            for j in range(0, len(fields)):
                d = data[i][j]
                if j < len(fields)-1:
                    if d is None:
                        valstr = valstr+"null,"
                    else:
                        if isinstance(d, str):
                            valstr = valstr+"'"+d+"',"
                        elif isinstance(d, (datetime.datetime, datetime.date, datetime.timedelta)):
                            valstr = valstr+"'"+str(d)+"',"
                        else:
                            valstr = valstr+str(d)+","
                else:
                    if d is None:
                        valstr = valstr+"null"
                    else:
                        if isinstance(d, str):
                            valstr = valstr+"'"+d+"'"
                        elif isinstance(d, (datetime.datetime, datetime.date, datetime.timedelta)):
                            valstr = valstr+"'"+str(d)+"'"
                        else:
                            valstr = valstr+str(d)
            if i < len(data)-1:
                valstr = valstr+"),"
            else:
                valstr = valstr+")"

        insert_sql = f"INSERT INTO {table_name} ({keystr}) VALUES {valstr}"
        cursor.execute(insert_sql)
        self.connection.commit()
        cursor.close()

    def __getFieldVale(self, feild):
        if feild.mysqlType == "bigint" or feild.mysqlType == "int" or feild.mysqlType == "smallint" or feild.mysqlType == "tinyint" or feild.mysqlType == "bit":
            if feild.value == "":
                return "null"
            else:
                return feild.value
        elif feild.mysqlType == "float" or feild.mysqlType == "double" or feild.mysqlType == "decimal":
            if feild.value == "":
                return "null"
            else:
                return feild.value
        else:
            if feild.mysqlType == "datetime" or feild.mysqlType == "datetime":
                if feild.value == "":
                    return "null"
                else:
                    return "'"+feild.value+"'"
            else:
                if feild.value == "":
                    return "''"
                else:
                    return "'"+feild.value+"'"

    def delete(self, table_name, id):
        cursor = self.connection.cursor()
        sql = f"delete FROM {table_name} WHERE id={id}"
        cursor.execute(sql)
        self.connection.commit()
        cursor.close()

    def update(self, table_name, data):
        cursor = self.connection.cursor()
        select_sql = f"SELECT count(*) FROM {table_name} WHERE id={data[0].value}"
        cursor.execute(select_sql)
        result = cursor.fetchone()
        dict_data = {}
        for d in data:
            dict_data[d.name] = self.__getFieldVale(d)
        if result[0] > 0:
            val_sql = ', '.join(
                [f"{key} = {dict_data[key]} " for key in dict_data.keys()])
            update_sql = f"UPDATE {table_name} set {val_sql} where id = {data[0].value}"
            cursor.execute(update_sql)
        else:
            fields = ', '.join([f"{key} " for key in dict_data.keys()])
            values = ', '.join(
                [f"{dict_data[key]}" for key in dict_data.keys()])
            insert_sql = f"INSERT INTO {table_name}({fields}) VALUE({values})"
            cursor.execute(insert_sql)
        self.connection.commit()
        cursor.close()
