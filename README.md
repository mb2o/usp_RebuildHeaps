# usp_RebuildHeaps

## SYNOPSIS    
A heap is a table without a clustered index. This proc can be used to rebuild those heaps on a database. Thereby alleviating the problems that arise from large numbers of forwarding records on a heap.

### Guideline for Performing Online Index Operations:

- Non-unique nonclustered indexes can be created online when the table contains LOB data types but none of these columns are used in the index definition as either key or non-key (included) columns. 
- Nonclustered indexes defined with LOB data type columns must be created or rebuilt offline.

### Transaction Log Considerations:

Large-scale index operations, performed offline or online, can generate large data loads that can cause the transaction log to quickly fill. To make sure that the index operation can be rolled back, the transaction log cannot be truncated until the index operation has been completed; however, the log can be backed up during the index operation. Therefore, the transaction log must have sufficient space to store both the index operation transactions and any concurrent user transactions for the duration of the index operation.

## DEPENDENCIES

None

## PARAMETERS

### Required

`@DatabaseName` specifies on which database the heaps should be rebuilt.              

### Optional

`@SchemaName` specifies the schema of a specific table you wish to target

`@TableName` specifies the name of a specific table you wish to target.

`@MinNumberOfPages` specifies the minimum number of pages required on the heap to be taken into account.

`@ProcessHeapCount` specifies the number of heaps that should be rebuilt. Processing large heaps can have a negative effect on the performance of your system. Also be aware that your log shipping processes can be greatly affected by rebuilding heaps as all changes need to be replicated.

`@MaxIndexCount` specifies the max number of nonclustered indexes a heap is allowed to have in order for it to be rebuilt. Rebuilding heaps with many indexes generates a lot of transaction log, which can have severe performance penalties.

`@MaxRowCount` specifies the number of rows that should not be exceeded for heaps you wish to rebuild.

`@MaxDOP` specifies maximum degree of parallelism.

`@RebuildTable` should be set to 1 when the worktable has to be rebuilt, e.g. after an update to the stored procedure when fields have changed.

`@DryRun` specifies whether the actual query should be executed or just printed to the screen.	

## NOTES

- When the working table is first created, execution ends. This leaves time for manipulation of the working table before actually doing the REBUILDs.


## USAGE     

``` sql
-- Rebuild the two heaps from the working table with the highest forwarded records count
EXEC dbo.usp_RebuildHeaps @DatabaseName = 'HIX_PROD',
                          @MaxDOP = 4,
                          @DryRun = 0;

-- Perform a targeted rebuild of the MEDCAT_RECDEEL table
EXEC dbo.usp_RebuildHeaps @DatabaseName = 'HIX_PROD',
                          @SchemaName = N'dbo',
                          @TableName = N'MEDICAT_RECDEEL',
                          @MaxDOP = 8,
                          @DryRun = 0;
   
-- Just rebuild the working table
EXEC dbo.usp_RebuildHeaps @DatabaseName = 'HIX_PROD',
                          @RebuildTable = 1;
                          
-- Rebuild the two heaps from the working table with the highest forwarded records count
-- that do not have more than 5 nonclustered indexes
EXEC dbo.usp_RebuildHeaps @DatabaseName = 'HIX_PROD',
                          @MaxIndexCount = 5,
						 @DryRun = 0;
```

