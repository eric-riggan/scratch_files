A recent question on Reddit asked a question about querying network equipment to obtain aggregated data about a user's traffic while connected to a VPN tunnel. The user's data is in a postgres database, which contains a table for the VPN sessions, and a table of traffic logs. The user eventually wants a report which can summarize the activity of a given user's VPN session in the form "User A logged in at 08:25, logged out at 13:27, sent 100MB, received 2.4GB, and RDP to this IP Address was the most used application".

The user also provided some limited details about the table structure. The VPN events table contains a username, remote IP Address, event ID (connect or disconnect), and a message. The traffic logs show a username, a source IP address, a destination IP Address, the name of an application, and the bytes sent and received for the session.

With the limited information available, we need to make some further assumptions about how the tables are laid out before we can mock them up. First, we need to assume that the traffic logs also contain a timestamp indicating when the traffic is taking place - ideally, a session beginning and end. Assuming this is the case, we can use the user ID as a bridge between the VPN events and the traffic log, using the start and end times of the VPN sessions as bounds on which to filter the traffic.

I don't have access to a postgres database, but I do have access to SQL Server so I'll be using T-SQL to perform the query.

First, let's mock up some tables and insert some dummy data since we don't have the source schema to work with.

``` sql
---------------------------------------------------------------
-- Create sample VPN Events table 
---------------------------------------------------------------

DROP TABLE IF EXISTS dbo.vpn_events;
CREATE TABLE dbo.vpn_events (
    row_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    user_name VARCHAR(50) NOT NULL,
    remote_ip_address VARCHAR(15) NOT NULL,
    event_id INT NOT NULL, -- 1 = connect, 2 = disconnect
    event_time DATETIME NOT NULL,
    insert_datetime DATETIME NOT NULL DEFAULT GETDATE(),
    update_datetime DATETIME NOT NULL DEFAULT GETDATE(),
    -- Check constraint on event_id
    CONSTRAINT CK_vpn_events_event_id CHECK (event_id IN (1,2))
)
GO
```

``` sql
---------------------------------------------------------------
-- Create sample VPN Events table 
---------------------------------------------------------------

DROP TABLE IF EXISTS dbo.vpn_events;
CREATE TABLE dbo.vpn_events (
    row_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    user_name VARCHAR(50) NOT NULL,
    remote_ip_address VARCHAR(15) NOT NULL,
    event_id INT NOT NULL, -- 1 = connect, 2 = disconnect
    event_time DATETIME NOT NULL,
    insert_datetime DATETIME NOT NULL DEFAULT GETDATE(),
    update_datetime DATETIME NOT NULL DEFAULT GETDATE(),
    -- Check constraint on event_id
    CONSTRAINT CK_vpn_events_event_id CHECK (event_id IN (1,2))
)
GO
```

``` sql
------------------------------------------------------------
-- Insert dummy data to vpn_events table
------------------------------------------------------------

DECLARE @user_id INT = 1
DECLARE @user_name VARCHAR(50) = 'user1'
DECLARE @remote_ip_address VARCHAR(15) = '192.168.1.2'
DECLARE @event_id INT = 1
DECLARE @event_time DATETIME = '2019-01-01 00:00:00.000'
DECLARE @insert_datetime DATETIME = GETDATE()
DECLARE @update_datetime DATETIME = GETDATE()

WHILE @user_id <= 5
BEGIN
    WHILE @event_id <= 2
    BEGIN
        INSERT INTO vpn_events (user_id, user_name, remote_ip_address, event_id, event_time, insert_datetime, update_datetime)
        VALUES (@user_id, @user_name, @remote_ip_address, @event_id, @event_time, @insert_datetime, @update_datetime)

        SET @event_id = @event_id + 1
    END

    SET @user_id = @user_id + 1
    SET @user_name = 'user' + CAST(@user_id AS VARCHAR(10))
    SET @event_id = 1
END

UPDATE vpn_events
   SET event_time = DATEADD(MINUTE, 30, event_time)
    WHERE event_id = 2;
GO
```

``` sql
--------------------------------------------------------------------------------
-- Insert dummy data to traffic_log 
--------------------------------------------------------------------------------

DECLARE @user_id INT = 1;
DECLARE @user_name VARCHAR(50) = 'user' + CAST(@user_id AS VARCHAR(10));
DECLARE @source_ip_address VARCHAR(15);
DECLARE @destination_ip_address VARCHAR(15) = '192.168.1.2';
DECLARE @min_session_start_time DATETIME = '2019-01-01 00:00:00.000';
DECLARE @max_session_end_time DATETIME = '2019-01-01 00:30:00.000';
DECLARE @session_start_time DATETIME;
DECLARE @session_end_time DATETIME;
DECLARE @bytes_sent BIGINT;
DECLARE @bytes_received BIGINT;
DECLARE @application_name VARCHAR(50);
DECLARE @insert_datetime DATETIME = GETDATE();
DECLARE @update_datetime DATETIME = GETDATE();

WHILE @user_id <= 5
BEGIN
    SET @source_ip_address = '10.1.1.' + CAST(@user_id AS VARCHAR(10));

    DECLARE @num_records INT = CAST(RAND() * 100 AS INT) + 1;
    DECLARE @record_num INT = 1;

    WHILE @record_num <= @num_records
    BEGIN
        SET @session_start_time = DATEADD(SECOND, CAST(RAND() * 1800 AS INT), @min_session_start_time);
        SET @session_end_time = DATEADD(SECOND, CAST(RAND() * 1800 AS INT), @session_start_time);
        SET @bytes_sent = CAST(RAND() * 1000000 AS BIGINT) + 1;
        SET @bytes_received = CAST(RAND() * 1000000 AS BIGINT) + 1;
        SET @application_name = 'app' + CAST(CAST(RAND() * 10 AS INT) + 1 AS VARCHAR(10));

        INSERT INTO traffic_log (user_id, user_name, source_ip_address, destination_ip_address, session_start_time, session_end_time, bytes_sent, bytes_received, application_name, insert_datetime, update_datetime)
        VALUES (@user_id, @user_name, @source_ip_address, @destination_ip_address, @session_start_time, @session_end_time, @bytes_sent, @bytes_received, @application_name, @insert_datetime, @update_datetime)

        SET @record_num = @record_num + 1
    END

    SET @user_id = @user_id + 1;
    SET @user_name = 'user' + CAST(@user_id AS VARCHAR(10));
END;
GO
```


``` sql
SELECT *
  FROM vpn_events;
```

<table>
    <thead>
        <tr>
            <th>row_id</th>
            <th>user_id</th>
            <th>user_name</th>
            <th>remote_ip_address</th>
            <th>event_id</th>
            <th>event_time</th>
            <th>insert_datetime</th>
            <th>update_datetime</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>1</td>
            <td>user1</td>
            <td>192.168.1.2</td>
            <td>1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>2</td>
            <td>1</td>
            <td>user1</td>
            <td>192.168.1.2</td>
            <td>2</td>
            <td>2019-01-01 00:30:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>3</td>
            <td>2</td>
            <td>user2</td>
            <td>192.168.1.2</td>
            <td>1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>4</td>
            <td>2</td>
            <td>user2</td>
            <td>192.168.1.2</td>
            <td>2</td>
            <td>2019-01-01 00:30:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>5</td>
            <td>3</td>
            <td>user3</td>
            <td>192.168.1.2</td>
            <td>1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>6</td>
            <td>3</td>
            <td>user3</td>
            <td>192.168.1.2</td>
            <td>2</td>
            <td>2019-01-01 00:30:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>7</td>
            <td>4</td>
            <td>user4</td>
            <td>192.168.1.2</td>
            <td>1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>8</td>
            <td>4</td>
            <td>user4</td>
            <td>192.168.1.2</td>
            <td>2</td>
            <td>2019-01-01 00:30:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>9</td>
            <td>5</td>
            <td>user5</td>
            <td>192.168.1.2</td>
            <td>1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
        <tr>
            <td>10</td>
            <td>5</td>
            <td>user5</td>
            <td>192.168.1.2</td>
            <td>2</td>
            <td>2019-01-01 00:30:00</td>
            <td>2023-05-23 10:06:01.553000</td>
            <td>2023-05-23 10:06:01.553000</td>
        </tr>
    </tbody>
</table>


``` sql
SELECT *
  FROM traffic_log;
```

<table>
    <thead>
        <tr>
            <th>user_id</th>
            <th>user_name</th>
            <th>source_ip_address</th>
            <th>destination_ip_address</th>
            <th>session_start_time</th>
            <th>session_end_time</th>
            <th>bytes_sent</th>
            <th>bytes_received</th>
            <th>application_name</th>
            <th>insert_datetime</th>
            <th>update_datetime</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:20:16</td>
            <td>2019-01-01 00:49:07</td>
            <td>258829</td>
            <td>895403</td>
            <td>app4</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:01:04</td>
            <td>2019-01-01 00:30:05</td>
            <td>292017</td>
            <td>358104</td>
            <td>app6</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:20:07</td>
            <td>2019-01-01 00:41:21</td>
            <td>711804</td>
            <td>672181</td>
            <td>app10</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:24:55</td>
            <td>2019-01-01 00:28:21</td>
            <td>3471</td>
            <td>589580</td>
            <td>app3</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:27:33</td>
            <td>2019-01-01 00:38:03</td>
            <td>975818</td>
            <td>95381</td>
            <td>app10</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:16:40</td>
            <td>2019-01-01 00:26:53</td>
            <td>131419</td>
            <td>820550</td>
            <td>app6</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:17:23</td>
            <td>2019-01-01 00:39:52</td>
            <td>421969</td>
            <td>520916</td>
            <td>app4</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:26:08</td>
            <td>2019-01-01 00:44:51</td>
            <td>862216</td>
            <td>991691</td>
            <td>app8</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:12:04</td>
            <td>2019-01-01 00:19:25</td>
            <td>928695</td>
            <td>276617</td>
            <td>app9</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>10.1.1.1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:16:10</td>
            <td>2019-01-01 00:16:56</td>
            <td>854665</td>
            <td>471715</td>
            <td>app2</td>
            <td>2023-05-23 10:16:49.900000</td>
            <td>2023-05-23 10:16:49.900000</td>
        </tr>
    </tbody>
</table>



The first step is to transform VPN events into list of VPN Sessions by user.
We first identify the beginning of every session by locating all rows with the
appropriate event_id (1, in this case).

``` sql
SELECT ve.user_id,
       ve.user_name,
       ve.remote_ip_address,
       ve.event_time AS session_start_time
  FROM dbo.vpn_events AS ve
 WHERE ve.event_id = 1 -- Connect Event
```

<table>
    <thead>
        <tr>
            <th>user_id</th>
            <th>user_name</th>
            <th>remote_ip_address</th>
            <th>session_start_time</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
        </tr>
        <tr>
            <td>2</td>
            <td>user2</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
        </tr>
        <tr>
            <td>3</td>
            <td>user3</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
        </tr>
        <tr>
            <td>4</td>
            <td>user4</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
        </tr>
        <tr>
            <td>5</td>
            <td>user5</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
        </tr>
    </tbody>
</table>



To that, we join back to vpn_events, selecting the most recent (minimum) event time
that is both a disconnect event and is after the connect event.

``` sql
WITH session_starts AS (
    SELECT ve.user_id,
           ve.user_name,
           ve.remote_ip_address,
           ve.event_time AS session_start_time
      FROM dbo.vpn_events AS ve
     WHERE ve.event_id = 1 -- Connect Event
)
SELECT ve.user_id,
       ve.user_name,
       ve.remote_ip_address,
       ss.session_start_time,
       MIN(ve.event_time) AS session_end_time
  FROM dbo.vpn_events AS ve
       LEFT JOIN session_starts AS ss
              ON ve.user_id = ss.user_id
 WHERE ve.event_id = 2 -- Disconnect Event
   AND ve.event_time > ss.session_start_time
GROUP BY ve.user_id,
         ve.user_name,
         ve.remote_ip_address,
         ss.session_start_time
```

<table>
    <thead>
        <tr>
            <th>user_id</th>
            <th>user_name</th>
            <th>remote_ip_address</th>
            <th>session_start_time</th>
            <th>session_end_time</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
        </tr>
        <tr>
            <td>2</td>
            <td>user2</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
        </tr>
        <tr>
            <td>3</td>
            <td>user3</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
        </tr>
        <tr>
            <td>4</td>
            <td>user4</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
        </tr>
        <tr>
            <td>5</td>
            <td>user5</td>
            <td>192.168.1.2</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
        </tr>
    </tbody>
</table>



Now that we have our sessions identified, we can select all the traffic_log
data for each user within the session window. 

``` sql
SELECT sl.user_id,
       sl.user_name,
       sl.session_start_time AS vpn_connection_start,
       sl.session_end_time AS vpn_connection_end,
       DATEDIFF(SECOND, sl.session_start_time, sl.session_end_time) AS vpn_connection_duration,
       tl.source_ip_address,
       tl.application_name,
       tl.session_start_time AS application_session_start,
       tl.session_end_time AS application_session_end,
       tl.bytes_sent,
       tl.bytes_received
  FROM session_log AS sl
       INNER JOIN dbo.traffic_log AS tl
               ON sl.user_id = tl.user_id
 WHERE tl.session_start_time >= sl.session_start_time
   AND tl.session_end_time <= sl.session_end_time
```


<table>
    <thead>
        <tr>
            <th>user_id</th>
            <th>user_name</th>
            <th>vpn_connection_start</th>
            <th>vpn_connection_end</th>
            <th>vpn_connection_duration</th>
            <th>source_ip_address</th>
            <th>application_name</th>
            <th>application_session_start</th>
            <th>application_session_end</th>
            <th>bytes_sent</th>
            <th>bytes_received</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app3</td>
            <td>2019-01-01 00:24:55</td>
            <td>2019-01-01 00:28:21</td>
            <td>3471</td>
            <td>589580</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app6</td>
            <td>2019-01-01 00:16:40</td>
            <td>2019-01-01 00:26:53</td>
            <td>131419</td>
            <td>820550</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app9</td>
            <td>2019-01-01 00:12:04</td>
            <td>2019-01-01 00:19:25</td>
            <td>928695</td>
            <td>276617</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app2</td>
            <td>2019-01-01 00:16:10</td>
            <td>2019-01-01 00:16:56</td>
            <td>854665</td>
            <td>471715</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app4</td>
            <td>2019-01-01 00:02:05</td>
            <td>2019-01-01 00:12:05</td>
            <td>716202</td>
            <td>868560</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app5</td>
            <td>2019-01-01 00:21:40</td>
            <td>2019-01-01 00:24:05</td>
            <td>691476</td>
            <td>620131</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app4</td>
            <td>2019-01-01 00:00:21</td>
            <td>2019-01-01 00:19:16</td>
            <td>217640</td>
            <td>299012</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app10</td>
            <td>2019-01-01 00:16:02</td>
            <td>2019-01-01 00:29:28</td>
            <td>709219</td>
            <td>483821</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app7</td>
            <td>2019-01-01 00:07:21</td>
            <td>2019-01-01 00:09:24</td>
            <td>294657</td>
            <td>516682</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app7</td>
            <td>2019-01-01 00:05:30</td>
            <td>2019-01-01 00:06:48</td>
            <td>652399</td>
            <td>256018</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app5</td>
            <td>2019-01-01 00:16:21</td>
            <td>2019-01-01 00:22:07</td>
            <td>65446</td>
            <td>495295</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app2</td>
            <td>2019-01-01 00:12:38</td>
            <td>2019-01-01 00:21:20</td>
            <td>422880</td>
            <td>450737</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app4</td>
            <td>2019-01-01 00:16:21</td>
            <td>2019-01-01 00:19:36</td>
            <td>714076</td>
            <td>600353</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app2</td>
            <td>2019-01-01 00:21:10</td>
            <td>2019-01-01 00:25:46</td>
            <td>598306</td>
            <td>37835</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app9</td>
            <td>2019-01-01 00:00:41</td>
            <td>2019-01-01 00:29:57</td>
            <td>819642</td>
            <td>644198</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app7</td>
            <td>2019-01-01 00:14:55</td>
            <td>2019-01-01 00:18:39</td>
            <td>765666</td>
            <td>893673</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app5</td>
            <td>2019-01-01 00:03:41</td>
            <td>2019-01-01 00:07:08</td>
            <td>70878</td>
            <td>832883</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app9</td>
            <td>2019-01-01 00:16:43</td>
            <td>2019-01-01 00:21:27</td>
            <td>36310</td>
            <td>135630</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app3</td>
            <td>2019-01-01 00:09:55</td>
            <td>2019-01-01 00:21:50</td>
            <td>885184</td>
            <td>576716</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app9</td>
            <td>2019-01-01 00:14:15</td>
            <td>2019-01-01 00:15:11</td>
            <td>397114</td>
            <td>61323</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app1</td>
            <td>2019-01-01 00:07:31</td>
            <td>2019-01-01 00:22:15</td>
            <td>229011</td>
            <td>336171</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app2</td>
            <td>2019-01-01 00:09:09</td>
            <td>2019-01-01 00:26:56</td>
            <td>622867</td>
            <td>327302</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app7</td>
            <td>2019-01-01 00:05:55</td>
            <td>2019-01-01 00:08:04</td>
            <td>528219</td>
            <td>680620</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app1</td>
            <td>2019-01-01 00:06:03</td>
            <td>2019-01-01 00:12:40</td>
            <td>590288</td>
            <td>716178</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app5</td>
            <td>2019-01-01 00:14:53</td>
            <td>2019-01-01 00:18:15</td>
            <td>747824</td>
            <td>405562</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app1</td>
            <td>2019-01-01 00:03:43</td>
            <td>2019-01-01 00:20:09</td>
            <td>64894</td>
            <td>580233</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app2</td>
            <td>2019-01-01 00:04:22</td>
            <td>2019-01-01 00:11:40</td>
            <td>89754</td>
            <td>657103</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app3</td>
            <td>2019-01-01 00:05:23</td>
            <td>2019-01-01 00:24:25</td>
            <td>110133</td>
            <td>501287</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app3</td>
            <td>2019-01-01 00:09:54</td>
            <td>2019-01-01 00:26:28</td>
            <td>599668</td>
            <td>535040</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app1</td>
            <td>2019-01-01 00:24:15</td>
            <td>2019-01-01 00:25:46</td>
            <td>917613</td>
            <td>159512</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app8</td>
            <td>2019-01-01 00:17:06</td>
            <td>2019-01-01 00:29:33</td>
            <td>469388</td>
            <td>406716</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app9</td>
            <td>2019-01-01 00:22:07</td>
            <td>2019-01-01 00:26:36</td>
            <td>676886</td>
            <td>799239</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app2</td>
            <td>2019-01-01 00:16:16</td>
            <td>2019-01-01 00:24:47</td>
            <td>914358</td>
            <td>101345</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app4</td>
            <td>2019-01-01 00:03:16</td>
            <td>2019-01-01 00:04:09</td>
            <td>131831</td>
            <td>427899</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app7</td>
            <td>2019-01-01 00:01:03</td>
            <td>2019-01-01 00:05:54</td>
            <td>313505</td>
            <td>892308</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app9</td>
            <td>2019-01-01 00:03:01</td>
            <td>2019-01-01 00:16:52</td>
            <td>930547</td>
            <td>580489</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app8</td>
            <td>2019-01-01 00:15:55</td>
            <td>2019-01-01 00:21:29</td>
            <td>594996</td>
            <td>481135</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app8</td>
            <td>2019-01-01 00:11:33</td>
            <td>2019-01-01 00:15:12</td>
            <td>999709</td>
            <td>126688</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app10</td>
            <td>2019-01-01 00:25:15</td>
            <td>2019-01-01 00:28:26</td>
            <td>590717</td>
            <td>917074</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app4</td>
            <td>2019-01-01 00:07:14</td>
            <td>2019-01-01 00:23:19</td>
            <td>974871</td>
            <td>24520</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app6</td>
            <td>2019-01-01 00:02:58</td>
            <td>2019-01-01 00:04:31</td>
            <td>430398</td>
            <td>969152</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app6</td>
            <td>2019-01-01 00:05:53</td>
            <td>2019-01-01 00:27:06</td>
            <td>165867</td>
            <td>320793</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app7</td>
            <td>2019-01-01 00:02:51</td>
            <td>2019-01-01 00:13:52</td>
            <td>895434</td>
            <td>40632</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app8</td>
            <td>2019-01-01 00:01:25</td>
            <td>2019-01-01 00:01:32</td>
            <td>334665</td>
            <td>634033</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app10</td>
            <td>2019-01-01 00:02:05</td>
            <td>2019-01-01 00:04:01</td>
            <td>334240</td>
            <td>71319</td>
        </tr>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>10.1.1.1</td>
            <td>app3</td>
            <td>2019-01-01 00:22:18</td>
            <td>2019-01-01 00:26:21</td>
            <td>774423</td>
            <td>866448</td>
        </tr>
    </tbody>
</table>



Now we have all of the traffic_log records for our users during their VPN sessions, which we can aggregate
however we want. I opted to go with another CTE expression to roll up the usage by application before selecting
the first record which matches the condtion I'm after in a subquery, but other approaches exist which may be 
more suitable. My entire query is below.

``` sql
WITH session_starts AS (
    SELECT ve.user_id,
           ve.user_name,
           ve.remote_ip_address,
           ve.event_time AS session_start_time
      FROM dbo.vpn_events AS ve
     WHERE ve.event_id = 1 -- Connect Event
),
session_log AS (
SELECT ve.user_id,
       ve.user_name,
       ve.remote_ip_address,
       ss.session_start_time,
       MIN(ve.event_time) AS session_end_time
  FROM dbo.vpn_events AS ve
       LEFT JOIN session_starts AS ss
              ON ve.user_id = ss.user_id
 WHERE ve.event_id = 2 -- Disconnect Event
   AND ve.event_time > ss.session_start_time
GROUP BY ve.user_id,
         ve.user_name,
         ve.remote_ip_address,
         ss.session_start_time
),
-- Step 2 - join the session log to the traffic log.
-- Join on user_id, filter by traffic log records that have a session start and
-- end time between the vpn session start and end time. The session start and end
-- times are inclusive. The session duration is in seconds.
session_activity AS (
    SELECT sl.user_id,
           sl.user_name,
           sl.session_start_time AS vpn_connection_start,
           sl.session_end_time AS vpn_connection_end,
           DATEDIFF(SECOND, sl.session_start_time, sl.session_end_time) AS vpn_connection_duration,
           tl.source_ip_address,
           tl.application_name,
           tl.session_start_time AS application_session_start,
           tl.session_end_time AS application_session_end,
           tl.bytes_sent,
           tl.bytes_received
      FROM session_log AS sl
           INNER JOIN dbo.traffic_log AS tl
                   ON sl.user_id = tl.user_id
     WHERE tl.session_start_time >= sl.session_start_time
       AND tl.session_end_time <= sl.session_end_time
),
-- Step 3 - Within each session, identify the
-- application session with the longest duration.
-- The application session start and end times are inclusive.
usage AS (
    SELECT sa.user_id,
           sa.user_name,
           sa.vpn_connection_start,
           sa.vpn_connection_end,
           sa.vpn_connection_duration,
           sa.application_name,
           SUM(DATEDIFF(SECOND, sa.application_session_start, sa.application_session_end)) AS application_usage_seconds,
           SUM(sa.bytes_sent) AS bytes_sent,
           SUM(sa.bytes_received) AS bytes_received,
           SUM(sa.bytes_sent + sa.bytes_received) AS bytes_total
      FROM session_activity AS sa
     GROUP BY sa.user_id,
           sa.user_name,
           sa.vpn_connection_start,
           sa.vpn_connection_end,
           sa.vpn_connection_duration,
           sa.application_name
)
-- Step 4 - Summarize the usage by user and application. Get the application names
-- with the most bytes sent and longest duration per user per session
SELECT u.user_id,
       u.user_name,
       u.vpn_connection_start,
       u.vpn_connection_end,
       u.vpn_connection_duration,
       -- name of application with most bytes sent
       (SELECT TOP 1 u2.application_name
           FROM usage AS u2
          WHERE u2.user_id = u.user_id
            AND u2.vpn_connection_start = u.vpn_connection_start
          ORDER BY u2.bytes_sent DESC) AS most_used_app_by_bytes_sent,
       -- most bytes sent by an application
       (SELECT TOP 1 u2.bytes_sent
           FROM usage AS u2
          WHERE u2.user_id = u.user_id
            AND u2.vpn_connection_start = u.vpn_connection_start
          ORDER BY u2.bytes_sent DESC) AS bytes_sent_by_most_used_app,
       -- name of application with longest duration
       (SELECT TOP 1 u2.application_name
           FROM usage AS u2
           WHERE u2.user_id = u.user_id
           AND u2.vpn_connection_start = u.vpn_connection_start
           ORDER BY u2.application_usage_seconds DESC) AS most_used_app_by_duration,
         -- longest duration by an application
       (SELECT TOP 1 u2.application_usage_seconds
           FROM usage AS u2
           WHERE u2.user_id = u.user_id
           AND u2.vpn_connection_start = u.vpn_connection_start
           ORDER BY u2.application_usage_seconds DESC) AS duration_by_most_used_app
  FROM usage AS u
 GROUP BY u.user_id,
       u.user_name,
       u.vpn_connection_start,
       u.vpn_connection_end,
       u.vpn_connection_duration
```

<table>
    <thead>
        <tr>
            <th>user_id</th>
            <th>user_name</th>
            <th>vpn_connection_start</th>
            <th>vpn_connection_end</th>
            <th>vpn_connection_duration</th>
            <th>most_used_app_by_bytes_sent</th>
            <th>bytes_sent_by_most_used_app</th>
            <th>most_used_app_by_duration</th>
            <th>duration_by_most_used_app</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>user1</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>app9</td>
            <td>3789194</td>
            <td>app9</td>
            <td>3637</td>
        </tr>
        <tr>
            <td>2</td>
            <td>user2</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>app3</td>
            <td>4789150</td>
            <td>app5</td>
            <td>5101</td>
        </tr>
        <tr>
            <td>3</td>
            <td>user3</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>app2</td>
            <td>3646027</td>
            <td>app5</td>
            <td>6461</td>
        </tr>
        <tr>
            <td>4</td>
            <td>user4</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>app8</td>
            <td>5517490</td>
            <td>app7</td>
            <td>5663</td>
        </tr>
        <tr>
            <td>5</td>
            <td>user5</td>
            <td>2019-01-01 00:00:00</td>
            <td>2019-01-01 00:30:00</td>
            <td>1800</td>
            <td>app1</td>
            <td>1955960</td>
            <td>app1</td>
            <td>2053</td>
        </tr>
    </tbody>
</table>


