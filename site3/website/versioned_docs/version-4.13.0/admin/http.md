---
id: http
title: BookKeeper Admin REST API
---

This document introduces BookKeeper HTTP endpoints, which can be used for BookKeeper administration.
To use this feature, set `httpServerEnabled` to `true` in file `conf/bk_server.conf`.

## All the endpoints

Currently all the HTTP endpoints could be divided into these 5 components:
1. Heartbeat: heartbeat for a specific bookie.
2. Config: doing the server configuration for a specific bookie.
3. Ledger: HTTP endpoints related to ledgers.
4. Bookie: HTTP endpoints related to bookies.
5. AutoRecovery: HTTP endpoints related to auto recovery.

## Heartbeat

### Endpoint: /heartbeat
* Method: GET
* Description: Get heartbeat status for a specific bookie
* Response: 

| Code   | Description |
|:-------|:------------|
|200 | Successful operation |

## Config

### Endpoint: /api/v1/config/server_config
1. Method: GET
    * Description: Get value of all configured values overridden on local server config
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
1. Method: PUT
    * Description: Update a local server config
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |configName  | String | Yes | Configuration name(key) |
        |configValue | String | Yes | Configuration value(value) |
    * Body: 
        ```json
        {
           "configName1": "configValue1",
           "configName2": "configValue2"
        }
        ```
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

## Metrics

### Endpoint: /metrics
1. Method: GET
    * Description: Get all metrics by calling `writeAllMetrics()` of `statsProvider` internally
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

## Ledger

### Endpoint: /api/v1/ledger/delete/?ledger_id=&lt;ledger_id&gt;
1. Method: DELETE
    * Description: Delete a ledger.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |ledger_id  | Long | Yes | ledger id of the ledger.  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/ledger/list/?print_metadata=&lt;metadata&gt;
1. Method: GET
    * Description: List all the ledgers.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |print_metadata | Boolean | No |  whether print out metadata  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "ledgerId1": "ledgerMetadata1",
          "ledgerId2": "ledgerMetadata2",
          ...
        }
        ```

### Endpoint: /api/v1/ledger/metadata/?ledger_id=&lt;ledger_id&gt;
1. Method: GET
    * Description: Get the metadata of a ledger.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |ledger_id  | Long | Yes | ledger id of the ledger.  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "ledgerId1": "ledgerMetadata1"
        }
        ```    

### Endpoint: /api/v1/ledger/read/?ledger_id=&lt;ledger_id&gt;&start_entry_id=&lt;start_entry_id&gt;&end_entry_id=&lt;end_entry_id&gt;
1. Method: GET
    * Description: Read a range of entries from ledger.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |ledger_id       | Long | Yes| ledger id of the ledger.  |
        |start_entry_id  | Long | No | start entry id of read range. |
        |end_entry_id    | Long | No | end entry id of read range.  |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "entryId1": "entry content 1",
          "entryId2": "entry content 2",
          ...
        }
        ```    

## Bookie

### Endpoint: /api/v1/bookie/info
1. Method: GET
   * Description:  Get bookie info
   * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |501 | Not implemented |
   * Body:
      ```json
      {
         "freeSpace" : 0,
         "totalSpace" : 0
       }
      ```

### Endpoint: /api/v1/bookie/list_bookies/?type=&lt;type&gt;&print_hostnames=&lt;hostnames&gt;
1. Method: GET
    * Description:  Get all the available bookies.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |type            | String  | Yes |  value: "rw" or "ro" , list read-write/read-only bookies. |
        |print_hostnames | Boolean | No  |  whether print hostname of bookies. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "bookieSocketAddress1": "hostname1",
          "bookieSocketAddress2": "hostname2",
          ...
        }
        ```    

### Endpoint: /api/v1/bookie/list_bookie_info
1. Method: GET
    * Description:  Get bookies disk usage info of this cluster.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "bookieAddress" : {free: xxx, total: xxx},
          "bookieAddress" : {free: xxx, total: xxx},
          ...
          "clusterInfo" : {total_free: xxx, total: xxx}
        }
        ```    

### Endpoint: /api/v1/bookie/last_log_mark
1. Method: GET
    * Description:  Get the last log marker.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          JournalId1 : position1,
          JournalId2 : position2,
          ...
        }
        ```   

### Endpoint: /api/v1/bookie/list_disk_file/?file_type=&lt;type&gt;
1. Method: GET
    * Description:  Get all the files on disk of current bookie.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |type  | String | No | file type: journal/entrylog/index. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "journal files" : "filename1 filename2 ...",
          "entrylog files" : "filename1 filename2...",
          "index files" : "filename1 filename2 ..."
        }
        ```

### Endpoint: /api/v1/bookie/expand_storage
1. Method: PUT
    * Description:  Expand storage for a bookie.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/bookie/gc
1. Method: PUT
    * Description:  trigger gc for this bookie.
    * Response:  

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

1. Method: GET
    * Description:  whether force triggered Garbage Collection is running or not for this bookie. true for is running.
    * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Body:
       ```json
       {
          "is_in_force_gc" : "false"
       }
       ```

### Endpoint: /api/v1/bookie/gc_details
1. Method: GET
    * Description:  get details of Garbage Collection Thread, like whether it is in compacting, last compaction time, compaction counter, etc.
    * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Body:
       ```json
       [ {
          "forceCompacting" : false,
          "majorCompacting" : false,
          "minorCompacting" : false,
          "lastMajorCompactionTime" : 1544578144944,
          "lastMinorCompactionTime" : 1544578144944,
          "majorCompactionCounter" : 1,
          "minorCompactionCounter" : 0
        } ]
       ```

### Endpoint: /api/v1/bookie/state
1. Method: GET
   * Description:  Exposes the current state of bookie
   * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
   * Body:
      ```json
      {
         "running" : true,
         "readOnly" : false,
         "shuttingDown" : false,
         "availableForHighPriorityWrites" : true
       }
      ```

### Endpoint: /api/v1/bookie/is_ready
1. Method: GET
   * Description:  Return true if the bookie is ready
   * Response:

        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
        |503 | Bookie is not ready |
   * Body: &lt;empty&gt;


## Auto recovery

### Endpoint: /api/v1/autorecovery/bookie/
1. Method: PUT
    * Description:  Ledger data recovery for failed bookie
    * Body: 
        ```json
        {
          "bookie_src": [ "bookie_src1", "bookie_src2"... ],
          "bookie_dest": [ "bookie_dest1", "bookie_dest2"... ],
          "delete_cookie": <bool_value>
        }
        ```
    * Parameters:
     
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |bookie_src    | Strings | Yes | bookie source to recovery |
        |bookie_dest   | Strings | No  | bookie data recovery destination |
        |delete_cookie | Boolean | No  | Whether delete cookie |
    * Response: 
     
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/autorecovery/list_under_replicated_ledger/?missingreplica=&lt;bookie_address&gt;&excludingmissingreplica=&lt;bookie_address&gt;
1. Method: GET
    * Description:  Get all under replicated ledgers.
    * Parameters: 
    
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        |missingreplica          | String | No | missing replica bookieId |
        |excludingmissingreplica | String | No | exclude missing replica bookieId |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          [ledgerId1, ledgerId2...]
        }
        ```

### Endpoint: /api/v1/autorecovery/who_is_auditor
1. Method: GET
    * Description:  Get auditor bookie id.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
    * Response Body format:  
    
        ```json
        {
          "Auditor": "hostname/hostAddress:Port"
        }
        ```

### Endpoint: /api/v1/autorecovery/trigger_audit
1. Method: PUT
    * Description: Force trigger audit by resting the lostBookieRecoveryDelay.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/autorecovery/lost_bookie_recovery_delay
1. Method: GET
    * Description: Get lostBookieRecoveryDelay value in seconds.
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

1. Method: PUT
    * Description: Set lostBookieRecoveryDelay value in seconds.
    * Body: 
        ```json
        {
          "delay_seconds": <delay_seconds>
        }
        ```
    * Parameters:
     
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        | delay_seconds | Long | Yes |  set delay value in seconds. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |

### Endpoint: /api/v1/autorecovery/decommission
1. Method: PUT
    * Description: Decommission Bookie, Force trigger Audit task and make sure all the ledgers stored in the decommissioning bookie are replicated.
    * Body: 
        ```json
        {
          "bookie_src": <bookie_src>
        }
        ```
    * Parameters:
     
        | Name | Type | Required | Description |
        |:-----|:-----|:---------|:------------|
        | bookie_src | String | Yes |   Bookie src to decommission.. |
    * Response:  
    
        | Code   | Description |
        |:-------|:------------|
        |200 | Successful operation |
        |403 | Permission denied |
        |404 | Not found |
