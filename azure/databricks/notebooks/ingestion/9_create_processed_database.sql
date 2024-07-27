{"version":"NotebookV1","origId":767643462327169,"name":"9_create_processed_database","language":"sql","commands":[{"version":"CommandV1","origId":767643462327170,"guid":"07bb9e4d-1d80-473b-a480-354a33c90eb7","subtype":"command","commandType":"auto","position":1.0,"command":"create database if not exists  f1_processed\nlocation \"/mnt/formula1dljavi/processed\"","commandVersion":22,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"errorDetails":null,"baseErrorDetails":null,"workflows":[],"startTime":1673092499965,"submitTime":1673092499909,"finishTime":1673092501441,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"754b6125-3ef3-4dcd-b828-6084dbf0c8ae"},{"version":"CommandV1","origId":767643462327171,"guid":"b0fa9ff8-dbbd-4d8a-a91a-efc64cf74266","subtype":"command","commandType":"auto","position":2.0,"command":"desc database f1_processed;","commandVersion":3,"state":"finished","results":{"type":"table","data":[["Catalog Name","spark_catalog"],["Namespace Name","f1_processed"],["Comment",""],["Location","dbfs:/mnt/formula1dljavi/processed"],["Owner","root"]],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[{"name":"database_description_item","type":"\"string\"","metadata":"{\"comment\":\"name of the namespace info\"}"},{"name":"database_description_value","type":"\"string\"","metadata":"{\"comment\":\"value of the namespace info\"}"}],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"errorDetails":null,"baseErrorDetails":null,"workflows":[],"startTime":1673092535050,"submitTime":1673092535031,"finishTime":1673092535151,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",5]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"c5f83cf0-732f-451f-af50-302e57d89de1"},{"version":"CommandV1","origId":767643462327172,"guid":"6a3b034d-8d12-41c3-a7f4-a991938c0b04","subtype":"command","commandType":"auto","position":3.0,"command":"create database if not exists  f1_presentation\nlocation \"/mnt/formula1dljavi/presentation\"","commandVersion":6,"state":"finished","results":{"type":"table","data":[],"arguments":{},"addedWidgets":{},"removedWidgets":[],"schema":[],"overflow":false,"aggData":[],"aggSchema":[],"aggOverflow":false,"aggSeriesLimitReached":false,"aggError":"","aggType":"","plotOptions":null,"isJsonSchema":true,"dbfsResultPath":null,"datasetInfos":[],"columnCustomDisplayInfos":{},"metadata":{"isDbfsCommandResult":false}},"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"errorDetails":null,"baseErrorDetails":null,"workflows":[],"startTime":1673093228631,"submitTime":1673093228346,"finishTime":1673093228857,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[["table",0]],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"7e560a50-5e7c-41f5-9d5d-5abb619af84e"},{"version":"CommandV1","origId":767643462327190,"guid":"3fb6d070-24ab-4a8e-9914-bed275ff1de0","subtype":"command","commandType":"auto","position":4.0,"command":"","commandVersion":0,"state":"input","results":null,"resultDbfsStatus":"INLINED_IN_TREE","resultDbfsErrorMessage":null,"errorSummary":null,"errorTraceType":null,"error":null,"errorDetails":null,"baseErrorDetails":null,"workflows":[],"startTime":0,"submitTime":0,"finishTime":0,"collapsed":false,"bindings":{},"inputWidgets":{},"displayType":"table","width":"auto","height":"auto","xColumns":null,"yColumns":null,"pivotColumns":null,"pivotAggregation":null,"useConsistentColors":false,"customPlotOptions":{},"commentThread":[],"commentsVisible":false,"parentHierarchy":[],"diffInserts":[],"diffDeletes":[],"globalVars":{},"latestUser":"a user","latestUserId":null,"commandTitle":"","showCommandTitle":false,"hideCommandCode":false,"hideCommandResult":false,"isLockedInExamMode":false,"iPythonMetadata":null,"metadata":{},"streamStates":{},"datasetPreviewNameToCmdIdMap":{},"tableResultIndex":null,"listResultMetadata":[],"subcommandOptions":null,"contentSha256Hex":null,"nuid":"8445a30b-448f-4f1a-a2ac-e87f938d0cb6"}],"dashboards":[],"guid":"7504b3b8-cb52-49f7-b9c4-b6dd0ef58bfa","globalVars":{},"iPythonMetadata":null,"inputWidgets":{},"notebookMetadata":{"pythonIndentUnit":4},"reposExportFormat":"SOURCE","environmentMetadata":null}