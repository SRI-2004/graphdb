'use client';

import React, { useState, useMemo } from 'react';
import { QueryResult } from '@/hooks/useChat';
import TableViewer from './TableViewer';
import GraphViewer from './GraphViewer';

interface DataExplorerProps {
  queryResults: QueryResult[];
  graphSuggestions: Record<string, any>[];
  isProcessing: boolean;
}

// --- Main Data Explorer Component --- 
const DataExplorer: React.FC<DataExplorerProps> = ({ queryResults, graphSuggestions, isProcessing }) => {
  const [currentTableIndex, setCurrentTableIndex] = useState(0);
  const [currentGraphIndex, setCurrentGraphIndex] = useState(0);

  const totalResults = queryResults.length;
  const isInitialState = !isProcessing && totalResults === 0;

  React.useEffect(() => {
      if (totalResults === 0) {
         setCurrentTableIndex(0);
         setCurrentGraphIndex(0);
      } else {
          if(currentTableIndex >= totalResults) setCurrentTableIndex(totalResults - 1); 
          if(currentGraphIndex >= totalResults) setCurrentGraphIndex(totalResults - 1);
      }
  }, [totalResults]); 

  const handleNextTable = () => setCurrentTableIndex(prev => Math.min(prev + 1, totalResults > 0 ? totalResults - 1 : 0));
  const handlePrevTable = () => setCurrentTableIndex(prev => Math.max(prev - 1, 0));
  const handleNextGraph = () => setCurrentGraphIndex(prev => Math.min(prev + 1, totalResults > 0 ? totalResults - 1 : 0));
  const handlePrevGraph = () => setCurrentGraphIndex(prev => Math.max(prev - 1, 0));

  const selectedTableResult = totalResults > 0 ? queryResults[currentTableIndex] : undefined;
  const selectedGraphResult = totalResults > 0 ? queryResults[currentGraphIndex] : undefined;

  const relevantGraphSuggestion = useMemo(() => {
    if (!selectedGraphResult || !graphSuggestions || graphSuggestions.length === 0) {
      return null;
    }
    return graphSuggestions.find(suggestion => suggestion.objective === selectedGraphResult.objective) || null;
  }, [selectedGraphResult, graphSuggestions]);

  return (
    <div className="space-y-6">
      <TableViewer 
        result={selectedTableResult} 
        currentIndex={currentTableIndex} 
        totalCount={totalResults} 
        onNext={handleNextTable}
        onPrev={handlePrevTable}
        isProcessing={isProcessing} 
        isInitialState={isInitialState}
      />
       <GraphViewer 
        result={selectedGraphResult}
        graphSuggestion={relevantGraphSuggestion}
        currentIndex={currentGraphIndex}
        totalCount={totalResults}
        onNext={handleNextGraph}
        onPrev={handlePrevGraph}
        isProcessing={isProcessing} 
        isInitialState={isInitialState}
      />
    </div>
  );
};

export default DataExplorer;