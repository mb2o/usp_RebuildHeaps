/********************************************************************************************************
    
    NAME:           usp_RebuildHeaps

    SYNOPSIS:       A heap is a table without a clustered index. This proc can be used to 
                    rebuild those heaps on a database, thereby alleviating problems that arise 
                    from large numbers of forwarded records.

    DEPENDENCIES:   .

	PARAMETERS:     Required:
                    @DatabaseName specifies on which database the heaps should be rebuilt.
                    
                    Optional:
                    @SchemaName specifies the schema of a specific table you want to target.

					@TableName specifies the name of a specific table you want to target.
					
					@MinNumberOfPages specifies. the minimum number of pages required on the heap
                    to be taken into account.

                    @ProcessHeapCount specifies the number of heaps that should be rebuilt. 
                    Processing large heaps can have a negative effect on the performance
                    of your system. Also be aware that your logshipping processes can be greatly
                    affected by rebuilding heaps as all changes need to be replicated.
					
					@MaxIndexCount specifies the max number of nonclustered indexes a heap is allowed 
					to have in order for it to be rebuilt. Rebuilding heaps with many indexes generates 
					a lot of transaction log, which can have severe performance penalties.

					@MaxRowCount specifies the number of rows that should not be exceeded for heaps
					to be rebuilt.
					
					@MaxDOP specifies maximum degree of paralellism.

                    @RebuildTable should be set to 1 when the worktable has to be rebuilt,
					e.g. after an update to the stored procedure when fields have changed.
					
					@QuitAfterBuild should be set to 1 when you need to manipulate the working table
					before starting the actual table rebuilds.
					
                    @DryRun specifies whether the actual query should be executed or just 
                    printed to the screen.
	
	NOTES:          

    AUTHOR:         Mark Boomaars, http://www.bravisziekenhuis.nl
    
    CREATED:        2021-09-29
    
    VERSION:        1.3

    LICENSE:        MIT
    
    USAGE:          EXEC dbo.usp_RebuildHeaps
                        @DatabaseName = 'StackOverflow',
						@MaxDOP = 4,
						@DryRun = 0;

*********************************************************************************************************/

IF OBJECTPROPERTY (OBJECT_ID ('usp_RebuildHeaps'), 'IsProcedure') = 1
    DROP PROCEDURE dbo.usp_RebuildHeaps;
GO

CREATE PROC dbo.usp_RebuildHeaps @DatabaseName     NVARCHAR(100),
                                 @SchemaName       NVARCHAR(100) = NULL,
                                 @TableName        NVARCHAR(100) = NULL,
                                 @MinNumberOfPages INT           = 0,
                                 @ProcessHeapCount INT           = 3,
								 @MaxIndexCount	   INT			 = 64,
                                 @MaxRowCount      BIGINT        = NULL,
                                 @MaxDOP           INT           = NULL,
                                 @RebuildTable     BIT           = 0,
								 @QuitAfterBuild   BIT           = 0,
                                 @DryRun           BIT           = 0
AS
BEGIN
    SET NOCOUNT ON;

    -------------------------------------------------------------------------------
    -- Declare some internal variables
    -------------------------------------------------------------------------------
	
    DECLARE @db_id                  INT,
            @db_name                sysname       = @DatabaseName,
            @object_id              INT,
            @edition                VARCHAR(100),
            @is_enterprise          BIT,
            @schema_name            sysname,
            @table_name             sysname,
            @page_count             BIGINT,
            @record_count           BIGINT,
            @_index_count           INT,
            @i                      INT           = 0,
            @forwarded_record_count BIGINT,
            @_maxdop                INT,
            @sql                    NVARCHAR(MAX),
            @msg                    NVARCHAR(MAX),
            @_starttime             DATETIME,
            @_endtime               DATETIME,
            @EndMessage             NVARCHAR(MAX),
            @ErrorMessage           NVARCHAR(MAX),
            @EmptyLine              NVARCHAR(MAX) = CHAR (9),
            @Error                  INT           = 0,
            @ReturnCode             INT           = 0;

    -------------------------------------------------------------------------------
    -- Some basic validation
    -------------------------------------------------------------------------------

    IF @DatabaseName IS NULL
    BEGIN
        SET @ErrorMessage = N'The @DatabaseName parameter must be specified and cannot be NULL. Stopping execution...';
        RAISERROR ('%s', 16, 1, @ErrorMessage) WITH NOWAIT;
        SET @Error = @@ERROR;
        RAISERROR (@EmptyLine, 10, 1) WITH NOWAIT;
    END;

    IF @SchemaName IS NOT NULL
       AND @TableName IS NULL
    BEGIN
        SET @ErrorMessage = N'The @TableName parameter must be specified when @SchemaName is supplied. Stopping execution...';
        RAISERROR ('%s', 16, 1, @ErrorMessage) WITH NOWAIT;
        SET @Error = @@ERROR;
        RAISERROR (@EmptyLine, 10, 1) WITH NOWAIT;
    END;

    IF @SchemaName IS NULL
       AND @TableName IS NOT NULL
    BEGIN
        SET @ErrorMessage = N'The @SchemaName parameter must be specified when @TableName is supplied. Stopping execution...';
        RAISERROR ('%s', 16, 1, @ErrorMessage) WITH NOWAIT;
        SET @Error = @@ERROR;
        RAISERROR (@EmptyLine, 10, 1) WITH NOWAIT;
    END;

    IF @Error <> 0
    BEGIN
        SET @ReturnCode = @Error;
        GOTO Logging;
    END;

    SELECT @edition = CAST(SERVERPROPERTY ('Edition') AS NVARCHAR(100));
    IF @edition LIKE 'Enterprise%' SET @is_enterprise = 1;

    -- If working table should be rebuilt, drop it
    IF OBJECT_ID (N'FragmentedHeaps', N'U') IS NOT NULL
       AND @RebuildTable = 1
    BEGIN
        DROP TABLE dbo.FragmentedHeaps;
        RAISERROR ('Current working table dropped', 10, 1) WITH NOWAIT;
    END;

    -------------------------------------------------------------------------------
    -- Preparing our working table
    -------------------------------------------------------------------------------

    IF OBJECT_ID (N'FragmentedHeaps', N'U') IS NULL
    BEGIN
        RAISERROR ('Preparing our working table', 10, 1) WITH NOWAIT;

        CREATE TABLE dbo.FragmentedHeaps (
            object_id              INT     NOT NULL,
            schema_name            sysname NOT NULL,
            table_name             sysname NOT NULL,
            page_count             BIGINT  NOT NULL,
            record_count           BIGINT  NOT NULL,
            forwarded_record_count BIGINT  NOT NULL,
            index_count            INT     NOT NULL
        );

        DECLARE heapdb CURSOR STATIC FOR
        SELECT d.database_id,
               d.name
        FROM sys.databases AS d
        WHERE d.name = @db_name;

        OPEN heapdb;

        WHILE 1 = 1
        BEGIN
            FETCH NEXT FROM heapdb
            INTO @db_id,
                 @db_name;

            IF @@FETCH_STATUS <> 0 BREAK;

            -- Loop through all heaps
            RAISERROR ('Looping through all heaps', 10, 1) WITH NOWAIT;

            SET @sql = N'DECLARE heaps CURSOR GLOBAL STATIC FOR
					SELECT i.object_id
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

            EXECUTE sys.sp_executesql @stmt = @sql;

            OPEN heaps;

            WHILE 1 = 1
            BEGIN
                FETCH NEXT FROM heaps
                INTO @object_id;

                IF @@FETCH_STATUS <> 0 BREAK;

                SET @i += 1;
                SET @schema_name = OBJECT_SCHEMA_NAME (@object_id, @db_id);
                SET @table_name = OBJECT_NAME (@object_id, @db_id);

				SET @sql = N'SELECT @_index_count = COUNT(*) FROM ' + QUOTENAME (@db_name) + N'.sys.indexes 
					WHERE is_hypothetical = 0 AND index_id <> 0 AND object_id = ' + CONVERT(NVARCHAR(10), @object_id) + ';';
	
				EXEC sys.sp_executesql @Sql, N'@_index_count INT OUTPUT', @_index_count OUTPUT;

                INSERT INTO dbo.FragmentedHeaps (object_id,
                                                 schema_name,
                                                 table_name,
                                                 page_count,
                                                 record_count,
                                                 forwarded_record_count,
                                                 index_count)
                SELECT P.object_id,
                       @schema_name,
                       @table_name,
                       P.page_count,
                       P.record_count,
                       P.forwarded_record_count,
                       @_index_count
                FROM sys.dm_db_index_physical_stats (DB_ID (@db_name), @object_id, 0, NULL, 'DETAILED') AS P
                WHERE P.page_count > @MinNumberOfPages
                      AND P.forwarded_record_count > 0;

                SET @msg = CONCAT (
                               FORMAT (GETDATE (), 'yyyy-MM-dd HH:mm:ss'),
                               ': ',
                               'processed table [',
                               @schema_name,
                               '].[',
                               @table_name,
                               '] (',
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

        -- End execution when @QuitAfterBuild = 1
        IF @QuitAfterBuild = 1
			GOTO Logging;
    END;

    -------------------------------------------------------------------------------
    -- Starting actual hard work
    -------------------------------------------------------------------------------

	RAISERROR ('Starting actual hard work', 10, 1) WITH NOWAIT;

    -- Determine configured instance value for MaxDOP
    SELECT @_maxdop = CONVERT (INT, value_in_use)
    FROM sys.configurations
    WHERE name = 'max degree of parallelism';

    -- If @MaxDOP has not been specified, use instance value
    IF @MaxDOP IS NOT NULL 
		SET @_maxdop = @MaxDOP;

    -- Are we dealing with a dry run?
    IF @DryRun = 1
        RAISERROR ('Performing a dry run. Nothing will be executed ...', 10, 1) WITH NOWAIT;

    -- Targeted rebuild?
    IF @SchemaName IS NOT NULL AND @TableName IS NOT NULL
    BEGIN
        SET @sql = N'ALTER TABLE ' + QUOTENAME (@db_name) + N'.' + QUOTENAME (@SchemaName) + N'.'
                   + QUOTENAME (@TableName) + N' REBUILD';

        IF @is_enterprise = 1
            SET @sql += N' WITH (ONLINE = ON, MAXDOP = ' + CONVERT (NVARCHAR(1), @_maxdop) + N')';
        ELSE
            SET @sql += N' WITH (MAXDOP = ' + CONVERT (NVARCHAR(1), @_maxdop) + N');';

        IF @DryRun = 0
        BEGIN
            SET @_starttime = GETDATE ();
            EXECUTE sys.sp_executesql @stmt = @sql;
            SET @_endtime = GETDATE ();

            -- Determine duration
            SET @sql += CONCAT (
                            ' (executed in ',
                            CONVERT (VARCHAR(5), DATEDIFF (SECOND, @_starttime, @_endtime)),
                            ' seconds)'
                        );

            -- Remove this table from the working table
            DELETE FROM dbo.FragmentedHeaps
            WHERE schema_name = @SchemaName
                  AND table_name = @TableName;
        END;

        -- Log executed action and its duration
        RAISERROR (@sql, 10, 1) WITH NOWAIT;
    END;
    ELSE
    BEGIN
        SELECT @db_id = d.database_id
        FROM sys.databases AS d
        WHERE d.name = @db_name;

        DECLARE worklist CURSOR STATIC FOR
        SELECT TOP (@ProcessHeapCount) object_id,
                                       page_count,
                                       record_count,
                                       forwarded_record_count,
									   index_count
        FROM dbo.FragmentedHeaps
        WHERE 1 = 1
              AND ((@MaxRowCount IS NULL) OR (record_count <= @MaxRowCount))
			  AND index_count <= @MaxIndexCount
        ORDER BY forwarded_record_count DESC;

        OPEN worklist;

        WHILE 1 = 1
        BEGIN
            FETCH NEXT FROM worklist
            INTO @object_id,
                 @page_count,
                 @record_count,
                 @forwarded_record_count,
				 @_index_count;

            IF @@FETCH_STATUS <> 0 BREAK;

            SET @schema_name = OBJECT_SCHEMA_NAME (@object_id, @db_id);
            SET @table_name = OBJECT_NAME (@object_id, @db_id);
            SET @msg = CONCAT (
                           FORMAT (GETDATE (), 'yyyy-MM-dd HH:mm:ss'),
                           ': ',
                           'rebuilding [',
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

            IF @is_enterprise = 1
                SET @sql += N' WITH (ONLINE = ON, MAXDOP = ' + CONVERT (NVARCHAR(1), @_maxdop) + N')';
            ELSE
                SET @sql += N' WITH (MAXDOP = ' + CONVERT (NVARCHAR(1), @_maxdop) + N');';

            IF @DryRun = 0
            BEGIN
                SET @_starttime = GETDATE ();
                EXECUTE sys.sp_executesql @stmt = @sql;
                SET @_endtime = GETDATE ();

                -- Determine duration
                SET @sql += CONCAT (
                                ' (executed in ',
                                CONVERT (VARCHAR(5), DATEDIFF (SECOND, @_starttime, @_endtime)),
                                ' seconds)'
                            );
            END;

            -- Log executed action and its duration
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
    END;

    -- Delete working table when no rows present
    IF @DryRun = 0
    BEGIN
        DECLARE @rows INT = 0;
        SELECT @rows = COUNT (*)
        FROM dbo.FragmentedHeaps;

        IF @rows = 0
        BEGIN
            DROP TABLE dbo.FragmentedHeaps;
            RAISERROR ('No outstanding work. Cleaning up...', 10, 1) WITH NOWAIT;
        END;
    END;

    ----------------------------------------------------------------------------------------------------
    -- Log information
    ----------------------------------------------------------------------------------------------------

    Logging:
    SET @EndMessage = N'Date and time: ' + CONVERT (NVARCHAR(20), GETDATE (), 120);
    RAISERROR ('%s', 10, 1, @EndMessage) WITH NOWAIT;

    RAISERROR (@EmptyLine, 10, 1) WITH NOWAIT;

    IF @ReturnCode <> 0 RETURN @ReturnCode;

END;
