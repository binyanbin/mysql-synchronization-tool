import pymysql
import mysqlutils

# mysql工具 全量同步示例 单表
source = pymysql.connect(host="127.0.0.1",
                         user="root", passwd="root", db="test", charset='utf8')

target = mysqlutils.MySQL(host="127.0.0.1", user="root",
                          password="root", database="test2", port=3306)

sync_list = [{"table": "pl_branch", "keys": ["id", "name", "introduce", "phone", "status_id", "created_by", "created_time",
                                             "updated_by", "updated_time", "address", "longitudeAndlatitude", "active_time"]}]

def fullsycn(table_name: str, keys: list):
    pageindex = 0
    pageSize = 500
    try:
        find = True
        while find:
            cursor = source.cursor()
            sql = f"SELECT * FROM {table_name} limit {pageindex * pageSize},{pageSize}"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) == 0:
                find = False
            else:
                data = []
                for row in results:
                    data.append(row)
                target.insert(table_name=table_name, fields=keys, data=data)
                pageindex = pageindex + 1
    except Exception as e:
        print(e.args)


for t in sync_list:
    fullsycn(table_name=t["table"], keys=t["keys"])
