'use client';

import React, { useMemo } from 'react';
import { QueryResult } from '@/hooks/useChat'; // Assuming useChat exports this type
import {
    ColumnDef,
    flexRender,
    getCoreRowModel,
    useReactTable,
    createColumnHelper,
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
import { ArrowLeft, ArrowRight } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

// Type for dynamic data rows
type DataRow = Record<string, any>;
const columnHelper = createColumnHelper<DataRow>();

interface TableViewerProps {
    result: QueryResult | undefined;
    currentIndex: number;
    totalCount: number;
    onNext: () => void;
    onPrev: () => void;
    isProcessing: boolean;
    isInitialState: boolean; // Flag for initial empty state
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

const TableViewer: React.FC<TableViewerProps> = ({ result, currentIndex, totalCount, onNext, onPrev, isProcessing, isInitialState }) => {
    const columns = useMemo<ColumnDef<DataRow, any>[]>(() => {
        if (!result?.dataframe || result.dataframe.length === 0) {
            return [];
        }
        return Object.keys(result.dataframe[0]).map(key => 
            columnHelper.accessor(key, {
                header: key, 
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
    }, [result?.dataframe]);

    const table = useReactTable({
        data: result?.dataframe ?? [],
        columns,
        getCoreRowModel: getCoreRowModel(),
    });

    const canGoPrev = currentIndex > 0;
    const canGoNext = currentIndex < totalCount - 1;

    // Determine content state
    const showLoadingPlaceholder = isProcessing && !result;
    const showInitialPlaceholder = isInitialState; 
    const showError = result?.error;
    const showNoData = result?.dataframe?.length === 0;
    const showTable = result?.dataframe && result.dataframe.length > 0;

    // Determine status text for overlay
    let statusText = "";
    if (showInitialPlaceholder) statusText = "Input query to generate table";
    if (showLoadingPlaceholder) statusText = "Generating table data...";

    return (
        <Card className="relative"> {/* Added relative for overlay positioning */} 
            <CardHeader>
                <div className="flex justify-between items-center">
                    {/* Use skeleton for title only in placeholder states */} 
                    {(showInitialPlaceholder || showLoadingPlaceholder) ? 
                        <Skeleton className="h-6 w-1/3" /> : 
                        <CardTitle>Table Viewer</CardTitle> 
                    }
                    {/* Conditionally render nav buttons only if needed and not in initial state */} 
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
                     {/* Skeleton for nav buttons in initial state */} 
                      {showInitialPlaceholder && totalCount <= 1 && (
                        <div className="flex items-center gap-2">
                             <Skeleton className="h-8 w-8 rounded-md" /> 
                             <Skeleton className="h-4 w-10" /> 
                             <Skeleton className="h-8 w-8 rounded-md" /> 
                        </div>
                    )}
                </div>
                 {/* Use skeleton for objective only in placeholder states */} 
                 {(showInitialPlaceholder || showLoadingPlaceholder) ? 
                     <Skeleton className="h-4 w-2/3 mt-1" /> : 
                     result && <p className="text-sm text-muted-foreground pt-1">Objective: {result.objective}</p>
                 } 
            </CardHeader>
            <CardContent className="min-h-[18rem]"> {/* Ensure min height */} 
                {/* Render Table or Placeholders */} 
                {(showInitialPlaceholder || showLoadingPlaceholder) ? (
                     <TablePlaceholderContent />
                ) : showError ? (
                    <div className="text-destructive bg-destructive/10 p-3 rounded border border-destructive/30 h-full flex items-center justify-center flex-col">
                        <p className='font-medium mb-1'>Error retrieving data for table:</p>
                        <pre className='text-sm whitespace-pre-wrap'>{result.error}</pre>
                    </div>
                ) : showTable ? (
                    <div className="rounded-md border max-h-72 overflow-auto relative"> 
                        <Table>
                            <TableHeader className="sticky top-0 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 z-10"> 
                                {table.getHeaderGroups().map((headerGroup) => (
                                    <TableRow key={headerGroup.id}>
                                        {headerGroup.headers.map((header) => (
                                            <TableHead key={header.id} className="whitespace-nowrap"> 
                                                {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                                            </TableHead>
                                        ))}
                                    </TableRow>
                                ))}
                            </TableHeader>
                            <TableBody>
                                {table.getRowModel().rows.map((row) => (
                                    <TableRow key={row.id} data-state={row.getIsSelected() && "selected"}>
                                        {row.getVisibleCells().map((cell) => (
                                            <TableCell key={cell.id} className="text-xs">
                                                {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                            </TableCell>
                                        ))}
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </div>
                ) : showNoData ? (
                    <p className="text-muted-foreground italic h-full flex items-center justify-center">Query executed successfully, but returned no data.</p>
                ) : null }

                 {/* Conditionally render query details only if result exists and not loading */} 
                 {result && !showLoadingPlaceholder && !showInitialPlaceholder && (
                    <details className='mt-3 text-xs'>
                        <summary className='cursor-pointer text-muted-foreground hover:text-foreground transition-colors'>Show Query</summary>
                        <pre className='text-xs bg-muted p-2 rounded mt-1 whitespace-pre-wrap font-mono'>{result.query}</pre>
                    </details>
                )}
            </CardContent>

             {/* Status Overlay - Show only for placeholder states */} 
             {(showInitialPlaceholder || showLoadingPlaceholder) && (
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none"> {/* Ensure overlay doesn't block header */} 
                    <p className="text-lg font-medium text-muted-foreground p-4 text-center bg-background/70 rounded-md">{statusText}</p>
                </div>
             )}
        </Card>
    );
};

export default TableViewer; 