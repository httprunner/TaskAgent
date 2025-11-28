# TaskAgent 飞书 API 使用文档

本文档总结了 TaskAgent 中使用的飞书多维表格（Bitable）API 接口。

## 1. 新增记录 (Create Record)

- **功能**: 向指定数据表中添加单条记录。
- **官方文档**: [新增记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/create)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records`
- **关键请求参数**:
    - `fields`: (Map) 记录的字段内容，Key 为字段名，Value 为字段值。
- **关键响应数据**:
    - `record`: (Object) 创建成功的记录信息，包含 `record_id` 和 `fields`。
- **接口频率限制**: 50 次/秒
- **代码实现**: `Client.CreateBitableRecord` (内部方法), `Client.CreateTaskRecord`, `Client.CreateResultRecord`

## 2. 更新记录 (Update Record)

- **功能**: 更新指定数据表中的单条记录。
- **官方文档**: [更新记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/update)
- **HTTP 方法**: `PUT`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/:record_id`
- **关键请求参数**:
    - `fields`: (Map) 需要更新的字段内容。
- **关键响应数据**:
    - `record`: (Object) 更新后的记录信息。
- **接口频率限制**: 50 次/秒
- **代码实现**: `Client.updateBitableRecord` (内部方法), `Client.UpdateTaskFields`, `Client.UpdateTaskStatus`

## 3. 查询记录 (Search Record)

- **功能**: 根据条件查询数据表中的记录。
- **官方文档**: [查询记录](https://open.larkoffice.com/document/docs/bitable-v1/app-table-record/search)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/search`
- **关键请求参数**:
    - `view_id`: (String) 视图 ID，可选。
    - `filter`: (Object) 筛选条件，可选。
    - `sort`: (Array) 排序条件，可选。
    - `page_size`: (Integer) 分页大小，默认 20，最大 500。
    - `page_token`: (String) 分页标记。
- **关键响应数据**:
    - `items`: (Array) 记录列表。
    - `has_more`: (Boolean) 是否还有更多记录。
    - `page_token`: (String) 下一页的分页标记。
    - `total`: (Integer) 总记录数。
- **接口频率限制**: 20 次/秒
- **代码实现**: `Client.listBitableRecords` (内部方法), `Client.FetchTaskTable`, `Client.FetchBitableRows`

## 4. 新增多条记录 (Batch Create Records)

- **功能**: 向指定数据表中批量添加记录。
- **官方文档**: [新增多条记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/batch_create)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/batch_create`
- **关键请求参数**:
    - `records`: (Array) 记录列表，每项包含 `fields`。
- **关键响应数据**:
    - `records`: (Array) 创建成功的记录列表，包含 `record_id`。
- **接口频率限制**: 50 次/秒
- **代码实现**: `Client.batchCreateBitableRecords` (内部方法), `Client.CreateTaskRecords`, `Client.CreateResultRecords`

## 5. 更新多条记录 (Batch Update Records)

- **功能**: 批量更新指定数据表中的记录。
- **官方文档**: [更新多条记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/batch_update)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/batch_update`
- **关键请求参数**:
    - `records`: (Array) 记录列表，每项需包含 `record_id` 和需要更新的 `fields`。
- **关键响应数据**:
    - `records`: (Array) 更新后的记录列表。
- **接口频率限制**: 50 次/秒
- **代码实现**: `Client.BatchUpdateBitableRecords`

## 6. 批量获取记录 (Batch Get Records)

- **功能**: 根据 record_id 列表批量获取记录详情。
- **官方文档**: [批量获取记录](https://open.larkoffice.com/document/docs/bitable-v1/app-table-record/batch_get)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/batch_get`
- **关键请求参数**:
    - `record_ids`: (Array) 需要获取的记录 ID 列表。
    - `user_id_type`: (String) 用户 ID 类型，可选。
- **关键响应数据**:
    - `records`: (Array) 获取到的记录列表。
    - `absent_record_ids`: (Array) 未找到的记录 ID。
    - `forbidden_record_ids`: (Array) 无权限的记录 ID。
- **接口频率限制**: 20 次/秒
- **代码实现**: `Client.BatchGetBitableRecords`, `Client.getBitableRecord` (内部方法，基于此接口封装)
