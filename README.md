# usp_RebuildHeaps
 
## SYNOPSIS    
A heap is a table without a clustered index. This proc can be used to rebuild those heaps on a database. Thereby alleviating the problems that arise from large numbers of forwarding records on a heap.

### Guideline for Performing Online Index Operations:

- Nonunique nonclustered indexes can be created online when the table contains LOB data types but none of these columns are used in the index definition as either key or nonkey (included) columns. 
- Nonclustered indexes defined with LOB data type columns must be created or rebuilt offline.

### Transaction Log Considerations:

Large-scale index operations, performed offline or online, can generate large data loads that can cause the transaction log to quickly fill. To make sure that the index operation can be rolled back, the transaction log cannot be truncated until the index operation has been completed; however, the log can be backed up during the index operation. Therefore, the transaction log must have sufficient space to store both the index operation transactions and any concurrent user transactions for the duration of the index operation.

## DEPENDENCIES

None

## PARAMETERS

### Required

`@DatabaseName` specifies on which database the heaps should be rebuilt.
                    
### Optional

`@MinNumberOfPages` specifies the minimum number of pages required on the heap to be taken into account

`@ProcessHeapCount` specifies the number of heaps that should be rebuilt. Processing large heaps can have a negative effect on the performance of your system. Also be aware that your logshipping processes can be greatly affected by rebuilding heaps as all changes need to be replicated.
					
`@RebuildOnlineOnly` specifies whether you only want to consider heaps that can be rebuilt online

`@MaxRowCount` specifies the number of rows that should not be exceeded for heaps you wish to rebuild

`@DryRun` specifies whether the actual query should be executed or just printed to the screen
	
## NOTES


## USAGE     

``` sql
EXEC dbo.usp_RebuildHeaps @DatabaseName = 'HIX_PRODUCTIE', @DryRun = 0;
```

## HISTORY

DATE       VERSION     AUTHOR               DESCRIPTION
========   =========   ==================   ======================================
20200103   1.0         Mark Boomaars		Open Sourced on GitHub
20200831   1.1         Mark Boomaars        Changes to logic and logging
20210122   1.2		   Mark Boomaars		Rebuild online when possible (Michiel vd Boogaard)