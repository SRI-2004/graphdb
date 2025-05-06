'use client';

import React, { useMemo, useState, useEffect } from 'react';
import { QueryResult } from '@/hooks/useChat'; // Assuming useChat exports this type
import {
    ColumnDef,
    flexRender,
    getCoreRowModel,
    useReactTable,
    createColumnHelper,
    SortingState,
    getSortedRowModel,
    RowSelectionState,
} from "@tanstack/react-table";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ArrowLeft, ArrowRight, ArrowUpDown } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";
import { Skeleton } from "@/components/ui/skeleton";

// Type for dynamic data rows
type DataRow = Record<string, unknown>;
const columnHelper = createColumnHelper<DataRow>();

interface TableViewerProps {
    result: QueryResult | undefined;
    currentIndex: number;
    totalCount: number;
    onNext: () => void;
    onPrev: () => void;
    isProcessing: boolean;
    isInitialState: boolean; // Flag for initial empty state
    onSetPendingContext: (context: { display: string; backend: string }) => void;
}

// New Placeholder Component (internal or could be separate)
const TablePlaceholderContent: React.FC = () => {
     // Define fixed heights matching the viewer components
    const tablePlaceholderHeight = "h-[18rem]"; // Corresponds to max-h-72
    return (
        <div className={`${tablePlaceholderHeight} filter blur-sm opacity-60 pointer-events-none`}> 
            {/* Basic table-like structure */}
            <div className="w-full h-full border rounded-md flex flex-col">
                {/* Placeholder Header Row */}
                <div className="flex border-b p-2 flex-shrink-0 bg-muted/50">
                    <Skeleton className="h-4 w-8 mr-2 bg-muted-foreground/20" />
                    <Skeleton className="h-4 flex-1 mr-2 bg-muted-foreground/20" />
                    <Skeleton className="h-4 flex-1 mr-2 bg-muted-foreground/20" />
                    <Skeleton className="h-4 flex-1 bg-muted-foreground/20" />
                </div>
                {/* Placeholder Body Rows */}
                <div className="p-2 space-y-2 flex-1 overflow-hidden">
                    <Skeleton className="h-4 w-full bg-muted-foreground/20" />
                    <Skeleton className="h-4 w-full bg-muted-foreground/20" />
                    <Skeleton className="h-4 w-3/4 bg-muted-foreground/20" />
                    <Skeleton className="h-4 w-full bg-muted-foreground/20" />
                    <Skeleton className="h-4 w-full bg-muted-foreground/20" />
                    <Skeleton className="h-4 w-1/2 bg-muted-foreground/20" />
                </div>
            </div>
        </div>
    );
};

const TableViewer: React.FC<TableViewerProps> = ({ result, currentIndex, totalCount, onNext, onPrev, isProcessing, isInitialState, onSetPendingContext }) => {
    // Log the received result prop
    console.log(`[TableViewer] Rendering with result for index ${currentIndex}:`, result);
    
    const [sorting, setSorting] = useState<SortingState>([]);
    const [rowSelection, setRowSelection] = useState<RowSelectionState>({});
    const [isContextStaged, setIsContextStaged] = useState<boolean>(false);

    useEffect(() => {
        setIsContextStaged(false);
        setRowSelection({});
    }, [currentIndex]);

    const data = useMemo(() => result?.dataframe ?? [], [result?.dataframe]);

    const columns = useMemo<ColumnDef<DataRow, unknown>[]>(() => {
        if (!result?.dataframe || result.dataframe.length === 0) {
            return [];
        }
        
        const selectionColumn: ColumnDef<DataRow> = {
            id: "select",
            header: ({ table }) => (
              <Checkbox
                checked={
                  table.getIsAllPageRowsSelected() ||
                  (table.getIsSomePageRowsSelected() && "indeterminate")
                }
                onCheckedChange={(value: boolean | 'indeterminate') => table.toggleAllPageRowsSelected(!!value)}
                aria-label="Select all"
              />
            ),
            cell: ({ row }) => (
              <Checkbox
                checked={row.getIsSelected()}
                onCheckedChange={(value: boolean | 'indeterminate') => row.toggleSelected(!!value)}
                aria-label="Select row"
              />
            ),
            enableSorting: false,
            enableHiding: false,
        };

        const dataColumns = Object.keys(result.dataframe[0]).map(key => 
            columnHelper.accessor(key, {
                header: ({ column }) => {
                  return (
                    <Button
                      variant="ghost"
                      onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
                      className="-ml-4"
                    >
                      {key}
                      <ArrowUpDown className="ml-2 h-4 w-4" />
                    </Button>
                  )
                }, 
                cell: info => {
                    const value = info.getValue();
                    if (typeof value === 'object' && value !== null) {
                        return <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(value)}</pre>;
                    }
                    if (typeof value === 'number') {
                        if (Math.abs(value) > 0.001 && !Number.isInteger(value)) {
                            return value.toFixed(2); 
                        }
                    }
                    return <div className="whitespace-nowrap">{String(value)}</div>;
                },
            })
        );

        return [selectionColumn, ...dataColumns];

    }, [result?.dataframe]);

    const table = useReactTable({
        data,
        columns,
        state: {
          sorting,
          rowSelection,
        },
        enableRowSelection: true,
        onRowSelectionChange: setRowSelection,
        onSortingChange: setSorting,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    const canGoPrev = currentIndex > 0;
    const canGoNext = currentIndex < totalCount - 1;

    // Determine content state
    const showLoadingPlaceholder = isProcessing && !result;
    const showInitialPlaceholder = isInitialState; 
    const showError = result?.error;
    const showNoData = data.length === 0 && !isInitialState && !isProcessing && !showError;
    const showTable = data.length > 0;

    // Determine status text for overlay
    let statusText = "";
    if (showInitialPlaceholder) statusText = "Input query to generate table";
    if (showLoadingPlaceholder) statusText = "Generating table data...";

    const handleSendSelectedContext = () => {
        console.log("[handleSendSelectedContext] Clicked.");
        const selectedRows = table.getSelectedRowModel().rows;
        const rowCount = selectedRows.length;
        console.log(`[handleSendSelectedContext] Initial rowCount: ${rowCount}`);
        
        if (rowCount === 0) {
            console.log("[handleSendSelectedContext] rowCount is 0, returning.");
            return;
        }

        const simpleContextString = "Context Added";

        const selectedRowsData = selectedRows.map(row => row.original);
        const headers = Object.keys(selectedRowsData[0]);
        const headerLine = `| ${headers.join(" | ")} |`;
        const separatorLine = `| ${headers.map(() => "---").join(" | ")} |`;
        const bodyLines = selectedRowsData.map(row => 
            `| ${headers.map(header => {
                const value = row[header];
                return typeof value === 'object' ? JSON.stringify(value) : String(value);
            }).join(" | ")} |`
        );
        const markdownTable = [
            headerLine,
            separatorLine,
            ...bodyLines,
        ].join("\n");

        console.log(`[handleSendSelectedContext] Calling onSetPendingContext with display: "${simpleContextString}" and backend data.`);
        onSetPendingContext({ display: simpleContextString, backend: markdownTable });
        
        console.log("[handleSendSelectedContext] Setting isContextStaged = true");
        setIsContextStaged(true); 
        
        console.log("[handleSendSelectedContext] Exiting.");
    };

    return (
        <Card className="relative flex flex-col">
            <CardHeader>
                <div className="flex justify-between items-center flex-wrap gap-2">
                    <div className='flex-shrink-0'>
                        {(showInitialPlaceholder || showLoadingPlaceholder) ? 
                            <Skeleton className="h-6 w-48" /> : 
                            <CardTitle>Table Viewer</CardTitle> 
                        }
                        {(showInitialPlaceholder || showLoadingPlaceholder) ? 
                            <Skeleton className="h-4 w-64 mt-1" /> : 
                            result && <p className="text-sm text-muted-foreground pt-1">Objective: {result.objective}</p>
                        } 
                    </div>
                    <div className='flex items-center gap-2 flex-shrink-0'>
                        {table.getSelectedRowModel().rows.length > 0 && (
                             <Button 
                                variant="outline" 
                                size="sm" 
                                onClick={handleSendSelectedContext}
                                disabled={isContextStaged}
                             >
                                 {isContextStaged ? 'Context Staged!' : `Stage Selected (${table.getSelectedRowModel().rows.length}) for Query`}
                             </Button>
                        )}
                        {totalCount > 1 && !showInitialPlaceholder && (
                            <div className="flex items-center gap-2">
                                <Button variant="outline" size="icon" onClick={onPrev} disabled={!canGoPrev || showLoadingPlaceholder}>
                                    <ArrowLeft className="h-4 w-4" />
                                </Button>
                                <span className="text-sm text-muted-foreground">
                                     {showLoadingPlaceholder ? "-" : `${currentIndex + 1} / ${totalCount}`}
                                </span>
                                <Button variant="outline" size="icon" onClick={onNext} disabled={!canGoNext || showLoadingPlaceholder}>
                                    <ArrowRight className="h-4 w-4" />
                                </Button>
                            </div>
                        )}
                        {showInitialPlaceholder && totalCount <= 1 && (
                            <div className="flex items-center gap-2">
                                 <Skeleton className="h-8 w-8 rounded-md" /> 
                                 <Skeleton className="h-4 w-10" /> 
                                 <Skeleton className="h-8 w-8 rounded-md" /> 
                            </div>
                        )}
                    </div>
                </div>
            </CardHeader>
            <CardContent className="flex-grow flex flex-col min-h-[18rem]">
                {(showInitialPlaceholder || showLoadingPlaceholder) ? (
                     <TablePlaceholderContent />
                ) : showError ? (
                    <div className="text-destructive bg-destructive/10 p-3 rounded border border-destructive/30 flex-grow flex items-center justify-center flex-col">
                        <p className='font-medium mb-1'>Error retrieving data for table:</p>
                        <pre className='text-sm whitespace-pre-wrap'>{result.error}</pre>
                    </div>
                ) : showTable ? (
                    <div className="flex-grow rounded-md border overflow-auto relative"> 
                        <Table>
                            <TableHeader className="sticky top-0 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 z-10"> 
                                {table.getHeaderGroups().map((headerGroup) => (
                                    <TableRow key={headerGroup.id}>
                                        {headerGroup.headers.map((header) => (
                                            <TableHead key={header.id} className="whitespace-nowrap px-2 py-2">
                                                {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                                            </TableHead>
                                        ))}
                                    </TableRow>
                                ))}
                            </TableHeader>
                            <TableBody>
                               {table.getRowModel().rows?.length ? (
                                  table.getRowModel().rows.map((row) => (
                                    <TableRow 
                                        key={row.id} 
                                        data-state={row.getIsSelected() && "selected"}
                                        className="hover:bg-muted/50"
                                    >
                                        {row.getVisibleCells().map((cell) => (
                                            <TableCell key={cell.id} className="text-xs px-2 py-1">
                                                {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                            </TableCell>
                                        ))}
                                    </TableRow>
                                  ))
                                ) : (
                                  <TableRow>
                                    <TableCell colSpan={columns.length} className="h-24 text-center">
                                      No results after filtering/sorting.
                                    </TableCell>
                                  </TableRow>
                                )}
                            </TableBody>
                        </Table>
                    </div>
                ) : showNoData ? (
                    <p className="text-muted-foreground italic flex-grow flex items-center justify-center">Query executed successfully, but returned no data.</p>
                ) : null }

                 {result && !showLoadingPlaceholder && !showInitialPlaceholder && (
                    <details className='mt-3 text-xs flex-shrink-0'> 
                        <summary className='cursor-pointer text-muted-foreground hover:text-foreground transition-colors'>Show Query</summary>
                        <pre className='text-xs bg-muted p-2 rounded mt-1 whitespace-pre-wrap font-mono'>{result.query}</pre>
                    </details>
                 )}
            </CardContent>

             {(showInitialPlaceholder || showLoadingPlaceholder) && (
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                    <p className="text-lg font-medium text-muted-foreground p-4 text-center bg-background/70 rounded-md">{statusText}</p>
                </div>
             )}
        </Card>
    );
};

export default TableViewer; 