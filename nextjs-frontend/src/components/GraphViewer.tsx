'use client';

import React, { useMemo } from 'react';
import { QueryResult } from '@/hooks/useChat';
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import type { PlotParams } from 'react-plotly.js';
import dynamic from 'next/dynamic';

// Dynamically import Plotly to avoid SSR issues
const Plot = dynamic(() => import('react-plotly.js'), { ssr: false });

// Define a more specific type for graph suggestions
interface GraphSuggestion {
    type?: string; 
    columns?: { 
        x?: string; 
        y?: string; 
        names?: string; 
        values?: string; 
        color?: string 
    }; 
    title?: string;
    // Allow other potential properties
    [key: string]: unknown; 
}

interface GraphViewerProps {
    result: QueryResult | undefined;
    graphSuggestion: GraphSuggestion | null; // Use the specific type
    currentIndex: number;
    totalCount: number;
    onNext: () => void;
    onPrev: () => void;
    isProcessing: boolean;
    isInitialState: boolean; // Flag for initial empty state
}

const GraphPlaceholderContent: React.FC = () => {
    const graphAreaHeight = "h-[350px]"; 
    return (
        <div className={`${graphAreaHeight} filter blur-sm opacity-60 pointer-events-none flex flex-col`}> 
            {/* Placeholder Plot Area with Axes and Grid */}
            <div className="flex-grow w-full border rounded-md flex p-2 pr-4 relative overflow-hidden"> 
                 {/* Y-Axis Skeleton */}
                 <Skeleton className="h-full w-4 mr-2 bg-muted-foreground/10" /> 
                 {/* Plot Content Area */}
                 <div className="flex-grow h-full relative"> 
                     {/* X-Axis Skeleton */}
                     <Skeleton className="absolute bottom-0 left-0 h-4 w-full bg-muted-foreground/10" /> 
                     {/* Simulated Grid Lines */}
                     <Skeleton className="absolute top-0 bottom-0 left-[25%] w-px bg-muted-foreground/10" />
                     <Skeleton className="absolute top-0 bottom-0 left-[50%] w-px bg-muted-foreground/10" />
                     <Skeleton className="absolute top-0 bottom-0 left-[75%] w-px bg-muted-foreground/10" />
                     <Skeleton className="absolute left-0 right-0 top-[25%] h-px bg-muted-foreground/10" />
                     <Skeleton className="absolute left-0 right-0 top-[50%] h-px bg-muted-foreground/10" />
                     <Skeleton className="absolute left-0 right-0 top-[75%] h-px bg-muted-foreground/10" />
                     {/* Restore Simulated Plot (or keep simple shape) */}
                     <div className="absolute bottom-[10%] left-[5%] w-[20%] h-[30%] border-b-2 border-l-2 border-primary/30 rounded-bl-lg"></div>
                     <div className="absolute bottom-[35%] left-[25%] w-[25%] h-[40%] border-t-2 border-l-2 border-primary/30 rounded-tl-lg"></div>
                     <div className="absolute bottom-[20%] left-[50%] w-[20%] h-[50%] border-b-2 border-r-2 border-primary/30 rounded-br-lg"></div>
                     <div className="absolute bottom-[60%] left-[70%] w-[25%] h-[25%] border-t-2 border-r-2 border-primary/30 rounded-tr-lg"></div>
                 </div>
             </div>
        </div>
    );
};

const GraphViewer: React.FC<GraphViewerProps> = ({ result, graphSuggestion, currentIndex, totalCount, onNext, onPrev, isProcessing, isInitialState }) => {

    const plotParams = useMemo<PlotParams | null>(() => {
        // Suggestion must exist and have a type other than 'none'
        if (!graphSuggestion || !graphSuggestion.type || graphSuggestion.type === 'none') {
             return null; 
        }

        const { type, columns, title } = graphSuggestion;
        const data = result?.dataframe; // Data comes from the QueryResult

        // Data must exist for the plot
        if (!data || data.length === 0) {
             // We have a suggestion, but no data for it yet
             // We'll handle displaying this info outside this useMemo
             return null; 
        }

        // Validation logic (keep as is)
        const xCol = columns?.x;
        const yCol = columns?.y;
        const nameCol = columns?.names;
        const valCol = columns?.values;
        const colorCol = columns?.color;

        // --- Customizations --- 
        const themeFontColor = 'hsl(var(--foreground))';
        const themeMutedColor = 'hsl(var(--muted-foreground))';
        const themeBorderColor = 'hsl(var(--border))';
        const themePrimaryColor = 'hsl(var(--primary))';
        const themeSecondaryColor = 'hsl(var(--secondary))'; 
        // Define a custom color sequence based on theme vars (adjust as needed)
        const customColorway = [
            themePrimaryColor,
            'hsl(var(--destructive))', // Example: using destructive for contrast
            'hsl(var(--chart-3))', // Assuming you have chart colors in globals.css
            'hsl(var(--chart-4))',
            'hsl(var(--chart-5))',
            themeSecondaryColor 
        ];
        // --- End Customizations ---

        const plotData: Partial<Plotly.PlotData>[] = [];
        // eslint-disable-next-line prefer-const
        let layout: Partial<Plotly.Layout> = {
            title: { 
                text: title || result?.objective || 'Generated Graph', 
                font: { size: 16, color: themeFontColor } // Larger title
            }, 
            xaxis: { 
                title: { text: xCol || '', font: { size: 12, color: themeMutedColor } }, 
                automargin: true, 
                gridcolor: themeBorderColor, // Subtle gridlines
                zerolinecolor: themeBorderColor,
                tickfont: { color: themeMutedColor, size: 10 }, // Smaller tick labels
                // showgrid: false, // Optional: hide gridlines
            }, 
            yaxis: { 
                title: { text: yCol || '', font: { size: 12, color: themeMutedColor } }, 
                automargin: true, 
                gridcolor: themeBorderColor, // Subtle gridlines
                zerolinecolor: themeBorderColor,
                tickfont: { color: themeMutedColor, size: 10 } // Smaller tick labels
            }, 
            margin: { l: 60, r: 30, t: 50, b: 50 }, // Adjusted margins
            height: 350, 
            autosize: true,
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            font: { color: themeFontColor, family: 'inherit' }, // Use inherited font family
            colorway: customColorway, // Apply custom color sequence
            hovermode: 'closest', // Standard hover mode
            legend: { // Legend styling (more relevant for multi-trace/pie)
                 orientation: "h", // Horizontal legend
                 yanchor: "bottom",
                 y: -0.2, // Position below plot
                 xanchor: 'center',
                 x: 0.5,
                 bgcolor: 'transparent',
                 font: { color: themeMutedColor, size: 10 }
             }
        };

        try { // Add try-catch for data access
            switch (type) {
                case 'bar':
                case 'line':
                case 'scatter':
                    if (!xCol || !yCol || !data[0]?.[xCol] || !data[0]?.[yCol]) return null; // Check columns exist in data
                    const xValues = data.map(row => row[xCol]) as Plotly.Datum[]; // Cast to Plotly.Datum[]
                    const yValues = data.map(row => row[yCol]) as Plotly.Datum[]; // Cast to Plotly.Datum[]
                    const trace: Partial<Plotly.PlotData> = { 
                        x: xValues, 
                        y: yValues, 
                        type: type === 'line' ? 'scatter' : type,
                        mode: type === 'line' ? 'lines+markers' : (type === 'scatter' ? 'markers' : undefined),
                        name: title || yCol, // Used in legend/tooltip
                        marker: { // Marker/bar styling
                             line: { // Border for bars/markers
                                 color: type === 'bar' ? 'hsl(var(--background))' : undefined, // Subtle border for bars
                                 width: type === 'bar' ? 1 : 0
                             },
                             opacity: 0.9, // Slightly transparent
                             size: type === 'scatter' ? 8 : undefined // Marker size for scatter
                        },
                        line: { // Line styling
                             width: type === 'line' ? 2 : undefined 
                        },
                        // --- Custom Hover Template --- 
                        hovertemplate: 
                            `<b>${xCol || 'X'}:</b> %{x}<br>` +
                            `<b>${yCol || 'Y'}:</b> %{y}<br>` +
                            // Add color value if colorCol exists
                            (colorCol && data[0]?.[colorCol] ? `<b>${colorCol}:</b> %{marker.color}<br>` : '') +
                            '<extra></extra>' // Hide default trace info
                    };
                    
                    // Handle custom color based on `colorCol` if provided
                    if (colorCol && data[0]?.[colorCol]) {
                        const colorValues = data.map(row => row[colorCol]) as Plotly.Color; // Cast to Plotly.Color
                        // Apply color directly to marker.color if it exists
                        trace.marker = { ...(trace.marker || {}), color: colorValues }; 
                        // Maybe add a color axis/bar if appropriate?
                        // layout.coloraxis = { colorscale: 'Viridis' }; // Example
                        // Remove layout.colorway if using marker.color for this trace
                        layout.colorway = undefined; 
                        // Add color info to hovertemplate (already done above)
                    } else {
                         // If not coloring by column, ensure the first color from the way is used
                         if (trace.marker) trace.marker.color = customColorway[0];
                         if (trace.line) trace.line.color = customColorway[0];
                    }
                    
                    plotData.push(trace);
                    break;

                case 'pie':
                    if (!nameCol || !valCol || !data[0]?.[nameCol] || !data[0]?.[valCol]) return null; // Check columns exist
                    plotData.push({ 
                        labels: data.map(row => row[nameCol]) as Plotly.Datum[], // Cast to Plotly.Datum[]
                        values: data.map(row => row[valCol]) as Plotly.Datum[], // Cast to Plotly.Datum[]
                        type: 'pie',
                        hole: 0.4, // Make it a donut chart
                        textfont: { 
                            size: 10, 
                            color: 'hsl(var(--background))' // Example: contrast against slice color
                        }, 
                        marker: { 
                            colors: customColorway,
                            line: { // Border between slices
                                color: 'hsl(var(--background))', // Use background color for separation
                                width: 1
                            }
                        },
                        // Set hoverinfo to 'none' and rely solely on hovertemplate
                        hoverinfo: 'none', 
                        textinfo: 'percent', 
                        // Remove layout.uniformtext as it's not a valid top-level layout property here
                        // layout.uniformtext = { mode: 'hide', minsize: 10 }; 
                        // --- Custom Hover Template --- 
                        hovertemplate: 
                            `<b>${nameCol || 'Category'}:</b> %{label}<br>` +
                            `<b>${valCol || 'Value'}:</b> %{value}<br>` +
                            `<b>Percent:</b> %{percent}<extra></extra>`
                    });
                    layout.xaxis = undefined;
                    layout.yaxis = undefined;
                    layout.showlegend = true; // Ensure legend is shown for pies
                    break;

                default:
                    console.warn(`Unsupported graph type in suggestion: ${type}`);
                    return null;
            }
        } catch (error) {
            console.error("Error processing graph suggestion data:", error);
            return null; // Return null on error accessing data
        }

        return {
            data: plotData,
            layout: layout,
            // Remove Plotly logo, adjust modebar buttons
            config: { 
                responsive: true, 
                displaylogo: false, 
                modeBarButtonsToRemove: ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d'] 
            },
            style: { width: '100%', height: '100%' },
            useResizeHandler: true,
        };

    }, [result?.dataframe, result?.objective, graphSuggestion]); // Dependencies remain the same

    const canGoPrev = currentIndex > 0;
    const canGoNext = currentIndex < totalCount - 1;
    
    // Determine plot possibility
    const plotPossible = plotParams !== null; 
    // Check if suggestion exists (and isn't 'none')
    const hasSuggestion = graphSuggestion && graphSuggestion.type !== 'none';
    // Check if suggestion is explicitly 'none'
    const suggestionIsNone = graphSuggestion?.type === 'none';
    // Check if there is data for the current result
    const hasData = result?.dataframe && result.dataframe.length > 0;

    // Determine content state - PRIORITIZE LOADING/INITIAL
    const showLoadingPlaceholder = isProcessing; // Show loading if backend is processing
    const showInitialPlaceholder = isInitialState; // Show initial if no query has run
    const showError = !isProcessing && result?.error;
    
    // Logic for when NOT loading/initial/error
    const showPlot = !isProcessing && !isInitialState && !showError && hasData && plotPossible;
    const showSuggestionButNoData = !isProcessing && !isInitialState && !showError && hasSuggestion && !hasData;
    const showNoSuggestion = !isProcessing && !isInitialState && !showError && !hasSuggestion;
    const showDataButNoPlot = !isProcessing && !isInitialState && !showError && hasData && !plotPossible && hasSuggestion; // Data exists, suggestion exists, but plotParams failed (e.g., bad cols)
    const showSuggestionIsNone = !isProcessing && !isInitialState && !showError && suggestionIsNone;
    const showNoDataMessage = !isProcessing && !isInitialState && !showError && !hasData && !hasSuggestion && !suggestionIsNone; // Query ran, no data, no suggestion

    // Determine status text for overlay or messages
    let statusText = "";
    if (showInitialPlaceholder) statusText = "Input query to generate graph";
    if (showLoadingPlaceholder) statusText = "Generating graph data..."; // Or just "Processing..."

    let contentMessage = "";
    if (showSuggestionButNoData) contentMessage = `Graph suggested (${graphSuggestion?.title || graphSuggestion?.type}), but no data was returned by the query.`;
    if (showNoSuggestion) contentMessage = "No specific graph suggested for this query result.";
    if (showDataButNoPlot) contentMessage = `Could not render suggested graph: ${graphSuggestion?.title || 'Check console for errors.'}`;
    if (showSuggestionIsNone) contentMessage = graphSuggestion?.title || "Agent indicated no specific graph is needed.";
    if (showNoDataMessage) contentMessage = "Query executed successfully, but returned no data.";

    return (
        <Card className="relative min-h-[400px]"> 
            <CardHeader>
                 <div className="flex justify-between items-center">
                    {/* Restore original header logic */}
                     {(showInitialPlaceholder || showLoadingPlaceholder) ? 
                        <Skeleton className="h-6 w-1/3" /> : 
                        <CardTitle>Graph Viewer</CardTitle>
                     }
                     {/* Restore pagination controls */} 
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
                     {/* Restore Skeleton for nav buttons */} 
                     {showInitialPlaceholder && totalCount <= 1 && (
                        <div className="flex items-center gap-2">
                             <Skeleton className="h-8 w-8 rounded-md" /> 
                             <Skeleton className="h-4 w-10" /> 
                             <Skeleton className="h-8 w-8 rounded-md" /> 
                        </div>
                    )}
                </div>
                 {/* Restore objective display */} 
                 {(showInitialPlaceholder || showLoadingPlaceholder) ? 
                    <Skeleton className="h-4 w-2/3 mt-1" /> : 
                    result && <p className="text-sm text-muted-foreground pt-1">Objective: {result.objective}</p>
                 }
            </CardHeader>
            <CardContent className="h-full flex flex-col"> 
                 {/* Restore conditional rendering logic */} 
                 {(showInitialPlaceholder || showLoadingPlaceholder) ? (
                    <GraphPlaceholderContent />
                 ) : showError ? (
                     <div className="text-destructive bg-destructive/10 p-3 rounded border border-destructive/30 h-full flex items-center justify-center flex-col">
                         <p className='font-medium mb-1'>Error retrieving/processing data:</p>
                         <pre className='text-sm whitespace-pre-wrap'>{result.error}</pre>
                     </div>
                 ) : showPlot ? (
                        <div className='border rounded p-2 flex-grow min-h-[360px]'> 
                            <Plot {...plotParams} /> 
                        </div>
                 ) : (
                     // Display other messages if not plotting
                     <p className="text-muted-foreground italic h-full flex items-center justify-center text-center p-4">{contentMessage}</p>
                 )}

                  {/* Restore query details */} 
                  {result && !showLoadingPlaceholder && !showInitialPlaceholder && (
                    <details className='mt-3 text-xs flex-shrink-0'> 
                        <summary className='cursor-pointer text-muted-foreground hover:text-foreground transition-colors'>Show Query</summary>
                        <pre className='text-xs bg-muted p-2 rounded mt-1 whitespace-pre-wrap font-mono'>{result.query}</pre>
                    </details>
                 )}
            </CardContent>
             {/* Restore Status Overlay */} 
             {(showInitialPlaceholder || showLoadingPlaceholder) && (
                <div className="absolute inset-0 flex items-center justify-center pointer-events-none"> 
                    <p className="text-lg font-medium text-muted-foreground p-4 text-center bg-background/70 rounded-md">{statusText}</p>
                </div>
             )}
        </Card>
    );
};

export default GraphViewer; 