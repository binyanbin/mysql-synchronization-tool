import time
from canal.client import Client
from canal.protocol import EntryProtocol_pb2
import mysqlutils

# mysql工具 基于canal增量同步示例
filter = b'test.pl_branch'

client = Client()
client.connect(host='127.0.0.1', port=11111)  
client.check_valid(username=b'', password=b'')  
client.subscribe(client_id=b'1001', destination=b'example',
                 filter=filter)

target = mysqlutils.MySQL(host="127.0.0.1", user="root",
                          password="root", database="test2", port=3306)

while True:
    message = client.get(100)
    # entries是每个循环周期内获取到数据集
    entries = message['entries']
    for entry in entries:
        datas = []
        entry_type = entry.entryType
        if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
            continue
        row_change = EntryProtocol_pb2.RowChange()
        row_change.MergeFromString(entry.storeValue)
        event_type = row_change.eventType
        header = entry.header
        # 数据库名
        database = header.schemaName
        # 表名
        table = header.tableName
        event_type = header.eventType
        for row in row_change.rowDatas:
            format_data = dict()
            if event_type == EntryProtocol_pb2.EventType.DELETE:
                target.delete(table_name=table, id=row.beforeColumns[0].value)
            elif event_type == EntryProtocol_pb2.EventType.INSERT or event_type == EntryProtocol_pb2.EventType.UPDATE:
                target.update(table_name=table, data=row.afterColumns)
            else:
                None
    time.sleep(1)
