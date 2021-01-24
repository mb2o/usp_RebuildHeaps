/********************************************************************************************************
    
    NAME:           usp_RebuildHeaps

    SYNOPSIS:       A heap is a table without a clustered index. This proc can be used to 
                    rebuild those heaps on a database. Thereby alleviating the problems that arise 
                    from large numbers of forwarding records on a heap.

    DEPENDENCIES:   .

	PARAMETERS:     Required:
                    @DatabaseName specifies on which database the heaps should be rebuilt.
                    
                    Optional:
                    @MinNumberOfPages specifies the minimum number of pages required on the heap
                    to be taken into account

                    @ProcessHeapCount specifies the number of heaps that should be rebuilt. 
                    Processing large heaps can have a negative effect on the performance
                    of your system. Also be aware that your logshipping processes can be greatly
                    affected by rebuilding heaps as all changes need to be replicated.
					
					@RebuildOnlineOnly specifies whether you only want to consider heaps that can
					be rebuilt online

					@MaxRowCount specifies the number of rows that should not be exceeded for heaps
					you wish to rebuild

                    @DryRun specifies whether the actual query should be executed or just 
                    printed to the screen
	
	NOTES:			

    AUTHOR:         Mark Boomaars, http://www.bravisziekenhuis.nl
    
    CREATED:        2020-01-03
    
    VERSION:        1.2

    LICENSE:        MIT
    
    USAGE:          EXEC dbo.usp_RebuildHeaps
                        @DatabaseName = 'HIX_PRODUCTIE', 
						@DryRun = 0;

*********************************************************************************************************/

IF OBJECTPROPERTY (OBJECT_ID ('usp_RebuildHeaps'), 'IsProcedure') = 1
    DROP PROCEDURE dbo.usp_RebuildHeaps;
GO

CREATE PROC dbo.usp_RebuildHeaps @DatabaseName      NVARCHAR(100),
                                 @MinNumberOfPages  INT     = 0,
                                 @ProcessHeapCount  INT     = 2,
                                 @RebuildOnlineOnly TINYINT = 0,
                                 @MaxRowCount       BIGINT  = NULL,
                                 @DryRun            TINYINT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET ARITHABORT ON;
    SET NUMERIC_ROUNDABORT OFF;

    DECLARE @db_id                  INT,
            @db_name                sysname       = @DatabaseName,
            @object_id              INT,
            @rebuild_online         TINYINT,
            @edition                VARCHAR(100),
            @is_enterprise          TINYINT,
            @schema_name            sysname,
            @table_name             sysname,
            @page_count             BIGINT,
            @record_count           BIGINT,
            @heap_count             INT,
            @i                      INT           = 0,
            @forwarded_record_count BIGINT,
            @sql                    NVARCHAR(MAX),
            @msg                    NVARCHAR(MAX),
            @EndMessage             NVARCHAR(MAX),
            @ErrorMessage           NVARCHAR(MAX),
            @EmptyLine              NVARCHAR(MAX) = CHAR (9),
            @Error                  INT           = 0,
            @ReturnCode             INT           = 0;

    IF @DatabaseName IS NULL
        BEGIN
            SET @ErrorMessage = N'The @DatabaseName parameter must be specified and cannot be NULL. Stopping execution...';
            RAISERROR ('%s', 16, 1, @ErrorMessage) WITH NOWAIT;
            SET @Error = @@ERROR;
            RAISERROR (@EmptyLine, 10, 1) WITH NOWAIT;
        END;

    SELECT @edition = CAST(SERVERPROPERTY ('Edition') AS NVARCHAR(100));
    IF @edition LIKE 'Enterprise%' SET @is_enterprise = 1;

    IF @Error <> 0
        BEGIN
            SET @ReturnCode = @Error;
            GOTO Logging;
        END;

    -------------------------------------------------------------------------------
    -- Preparing our working table
    -------------------------------------------------------------------------------
    IF OBJECT_ID (N'FragmentedHeaps', N'U') IS NULL
        BEGIN
            RAISERROR ('Preparing our working table', 10, 1) WITH NOWAIT;

            CREATE TABLE dbo.FragmentedHeaps (
                object_id              INT    NOT NULL,
                page_count             BIGINT NOT NULL,
                record_count           BIGINT NOT NULL,
                forwarded_record_count BIGINT NOT NULL,
                rebuild_online         TINYINT
            );

            DECLARE heapdb CURSOR STATIC FOR
            SELECT d.database_id, d.name
            FROM sys.databases AS d
            WHERE d.name = @db_name;

            OPEN heapdb;

            WHILE 1 = 1
                BEGIN
                    FETCH NEXT FROM heapdb
                    INTO @db_id, @db_name;

                    IF @@FETCH_STATUS <> 0 BREAK;

                    -- Loop through all heaps
                    RAISERROR ('Looping through all heaps', 10, 1) WITH NOWAIT;

                    SET @sql = N'DECLARE heaps CURSOR GLOBAL STATIC FOR
						SELECT i.object_id,
								ISNUMERIC (u.object_id)
						FROM ' + QUOTENAME (@db_name) + N'.sys.indexes AS i
						INNER JOIN ' + QUOTENAME (@db_name)
										+ N'.sys.objects AS o
							ON o.object_id = i.object_id
						LEFT OUTER JOIN ' + QUOTENAME (@db_name)
										+ N'.sys.indexes AS j
							ON j.object_id = i.object_id AND i.type_desc = ''HEAP''
						LEFT OUTER JOIN ' + QUOTENAME (@db_name)
										+ N'.sys.columns AS u
							ON u.object_id = j.object_id AND ((u.system_type_id IN (34, 35, 99, 241, 240)) OR (u.system_type_id IN (167, 231, 165) AND max_length = -1))
						WHERE i.type_desc = ''HEAP'' AND o.type_desc = ''USER_TABLE''
						GROUP BY i.object_id, u.object_id;';

                    EXECUTE sp_executesql @sql;

                    OPEN heaps;

                    WHILE 1 = 1
                        BEGIN
                            FETCH NEXT FROM heaps
                            INTO @object_id, @rebuild_online;

                            IF @@FETCH_STATUS <> 0 BREAK;

                            SET @i += 1;

                            INSERT INTO dbo.FragmentedHeaps (object_id,
                                                                page_count,
                                                                record_count,
                                                                forwarded_record_count,
                                                                rebuild_online)
                            SELECT P.object_id,
                                    P.page_count,
                                    P.record_count,
                                    P.forwarded_record_count,
                                    @rebuild_online
                            FROM sys.dm_db_index_physical_stats (DB_ID (@db_name), @object_id, 0, NULL, 'DETAILED') AS P
                            WHERE P.page_count > @MinNumberOfPages
                                    AND P.forwarded_record_count > 0;

                            -- Log tablename
                            SET @msg = CONCAT (
                                            'Added table [',
                                            OBJECT_SCHEMA_NAME (@object_id, DB_ID (@db_name)),
                                            '].[',
                                            OBJECT_NAME (@object_id, DB_ID (@db_name)),
                                            '] to worklist (',
                                            @i,
                                            ' of ',
                                            @@CURSOR_ROWS,
                                            ')'
                                        );
                            RAISERROR (@msg, 10, 1) WITH NOWAIT;
                        END;

                    CLOSE heaps;
                    DEALLOCATE heaps;
                END;

            CLOSE heapdb;
            DEALLOCATE heapdb;
        END;

    -------------------------------------------------------------------------------
    -- Starting actual hard work
    -------------------------------------------------------------------------------
    IF @DryRun = 1
        RAISERROR ('Performing a dry run. Nothing will be executed ...', 10, 1) WITH NOWAIT;

    RAISERROR ('Starting actual hard work', 10, 1) WITH NOWAIT;

    SELECT @db_id = d.database_id
    FROM sys.databases AS d
    WHERE d.name = @db_name;

    DECLARE worklist CURSOR STATIC FOR
    SELECT TOP (@ProcessHeapCount) object_id,
                                    page_count,
                                    record_count,
                                    forwarded_record_count,
                                    rebuild_online
    FROM dbo.FragmentedHeaps
    WHERE 1 = 1
            AND ((@RebuildOnlineOnly = 0) OR (rebuild_online = 1))
            AND ((@MaxRowCount IS NULL) OR (record_count <= @MaxRowCount))
    ORDER BY forwarded_record_count DESC;

    OPEN worklist;

    WHILE 1 = 1
        BEGIN
            FETCH NEXT FROM worklist
            INTO @object_id,
                    @page_count,
                    @record_count,
                    @forwarded_record_count,
                    @rebuild_online;

            IF @@FETCH_STATUS <> 0 BREAK;

            SET @schema_name = OBJECT_SCHEMA_NAME (@object_id, @db_id);
            SET @table_name = OBJECT_NAME (@object_id, @db_id);
            SET @msg = CONCAT (
                            'Rebuilding [',
                            @db_name,
                            '].[',
                            @schema_name,
                            '].[',
                            @table_name,
                            '] because of ',
                            @forwarded_record_count,
                            ' forwarded records.'
                        );

            RAISERROR (@msg, 10, 1) WITH NOWAIT;

            SET @sql = N'ALTER TABLE ' + QUOTENAME (@db_name) + N'.' + QUOTENAME (@schema_name) + N'.'
                        + QUOTENAME (@table_name) + N' REBUILD';

            IF @rebuild_online = 1 AND @is_enterprise = 1
                SET @sql += N' WITH (ONLINE = ON);';

            IF @DryRun = 0 EXECUTE sp_executesql @stmt = @sql;

            RAISERROR (@sql, 10, 1) WITH NOWAIT;

            -- Remove processed heap from working table
            IF @DryRun = 0
                BEGIN
                    DELETE FROM dbo.FragmentedHeaps
                    WHERE object_id = @object_id;

                    RAISERROR ('Removing heap from working table', 10, 1) WITH NOWAIT;
                END;
        END;

    CLOSE worklist;
    DEALLOCATE worklist;

    -- Delete working table when no rows present
    IF @DryRun = 0
        BEGIN
            DECLARE @rows INT = 0;
            SELECT @rows = COUNT (*)
            FROM dbo.FragmentedHeaps;

            IF @rows = 0
                BEGIN
                    DROP TABLE dbo.FragmentedHeaps;
                    RAISERROR ('No rows in table. Cleaning up...', 10, 1) WITH NOWAIT;
                END;
        END;

    ----------------------------------------------------------------------------------------------------
    -- Log information
    ----------------------------------------------------------------------------------------------------

    Logging:
    SET @EndMessage = N'Date and time: ' + CONVERT (NVARCHAR, GETDATE (), 120);
    RAISERROR ('%s', 10, 1, @EndMessage) WITH NOWAIT;

    RAISERROR (@EmptyLine, 10, 1) WITH NOWAIT;

    IF @ReturnCode <> 0
        BEGIN
            RETURN @ReturnCode;
        END;
END;