import pymysql
import mysqlutils

# mysql工具 全量同步示例
source = pymysql.connect(host="127.0.0.1",
                         user="root", passwd="root", db="test", charset='utf8')

target = mysqlutils.MySQL(host="127.0.0.1", user="root",
                          password="root", database="test2", port=3306)

sync_list = [{"table": "pl_branch", "keys": ["id", "name", "introduce", "phone", "status_id", "created_by", "created_time",
                                             "updated_by", "updated_time", "address", "longitudeAndlatitude", "active_time"]}]

split_table = 2
dict_data = {}


def fullsycn(table_name: str, keys: list,  size: int):
    pageindex = 0
    try:
        find = True
        while find:
            cursor = source.cursor()
            sql = f"SELECT * FROM {table_name} limit {pageindex * size},{size}"
            cursor.execute(sql)
            results = cursor.fetchall()
            if len(results) == 0:
                find = False
            else:
                for row in results:
                    m = row[0] % split_table
                    if dict_data.get(str(m)) is not None:
                        dict_data[str(m)].append(row)
                    else:
                        dict_data[str(m)] = []
                        dict_data[str(m)].append(row)
                pageindex = pageindex + 1
            for key in dict_data.keys():
                if len(dict_data[key]) > 0:
                    target.insert(table_name=table_name+key,
                                  fields=keys, data=dict_data[key])

    except Exception as e:
        print(e.args)


for t in sync_list:
    fullsycn(table_name=t["table"], keys=t["keys"], size=1000)
