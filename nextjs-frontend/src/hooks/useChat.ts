import { useState, useCallback, useEffect } from 'react';
import useWebSocket, { ReadyState } from 'react-use-websocket';

// Define the structure of a chat message
export interface ChatMessage {
  id: string; // Add unique ID for React keys
  role: 'user' | 'assistant' | 'system' | 'milestone'; // Add milestone role
  content: string; // Main text or summary for milestone OR fallback text
  // Optional fields based on backend stream
  reasoning?: string;
  reportSections?: { title: string; content: string }[]; // Store structured sections
  // Store the array of queries directly
  generatedQueries?: { objective: string; query: string }[]; 
  step?: string; // Associate milestone with a step
}

// Define the structure for query results from the backend
export interface QueryResult {
    objective: string;
    query: string;
    dataframe?: Record<string, any>[]; // Array of objects for table data
    error?: string;
}

// Define the structure of messages coming FROM the WebSocket
interface WebSocketMessage {
    type: string; // 'status', 'reasoning_summary', 'final_insight', 'final_recommendations', 'error', 'generated_queries', 'query_result' (NEW)
    // Add fields based on the backend stream types
    step?: string;
    status?: string;
    details?: string;
    reasoning?: string;
    insight?: string;
    // optimization_report?: string; // Replaced by report_sections
    report_sections?: { title: string; content: string }[]; // For structured reports
    suggestions?: Record<string, any>[]; // List of graph suggestions
    message?: string; // for errors
    generated_queries?: { objective: string; query: string }[]; // From backend status update
    // --- Fields for Query Results (Needs backend implementation) ---
    objective?: string;
    query?: string;
    data?: Record<string, any>[]; // Actual data from Neo4j
    error?: string; // Query execution error
    requires_execution?: boolean; // Added for workflows that might not need query execution
}

const WEBSOCKET_URL = 'ws://localhost:8000/api/v1/chat/stream'; // Ensure this matches your FastAPI backend

export function useChat() {
  // Main chat history (including milestones)
  const [messages, setMessages] = useState<ChatMessage[]>([
      {
        id: 'init_message',
        role: 'system',
        content: 'Hello! I am your Insight Assistant. Ask me to analyze your data or suggest optimizations.'
      }
  ]);
  // Data for the right-hand pane
  const [queryResults, setQueryResults] = useState<QueryResult[]>([]);
  // Live status update string
  const [currentStatus, setCurrentStatus] = useState<string | null>(null);
  // Processing state
  const [isProcessing, setIsProcessing] = useState<boolean>(false);
  // State for the graph suggestions (now a list)
  const [graphSuggestions, setGraphSuggestions] = useState<Record<string, any>[]>([]);

  const {
    sendMessage: sendWebSocketMessage,
    lastJsonMessage,
    readyState,
  } = useWebSocket<WebSocketMessage>(WEBSOCKET_URL, {
    share: false, // Each component instance gets its own connection (if needed, else true)
    shouldReconnect: (closeEvent) => true, // Automatically attempt to reconnect
    onOpen: () => { console.log('WebSocket Connected'); setCurrentStatus(null); setIsProcessing(false); },
    onClose: () => { console.log('WebSocket Disconnected'); setCurrentStatus('Connection closed.'); setIsProcessing(false); },
    onError: (event) => { console.error('WebSocket Error:', event); setCurrentStatus('Connection error.'); setIsProcessing(false); },
  });

  // Function to generate unique IDs
  const generateId = () => `msg_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;

  // --- Process incoming WebSocket messages --- 
  useEffect(() => {
    if (lastJsonMessage) {
      console.log('Received WS Message:', lastJsonMessage); // Debugging
      const { type, step, status } = lastJsonMessage;

      // Helper to update the most recent milestone message for a given step
      const updateLastMilestone = (stepName: string, updates: Partial<ChatMessage>) => {
        setMessages(prev => prev.map(msg => {
          if (msg.role === 'milestone' && msg.step === stepName && msg.id === prev.findLast(m => m.role === 'milestone' && m.step === stepName)?.id) {
            // Merge updates into the last matching milestone
            return { ...msg, ...updates };
          }
          return msg;
        }));
      };
      
      // Handle different message types from backend
      switch (type) {
        case 'status':
          const statusText = `**${step?.replace(/_/g, ' ')}**: ${status?.replace(/_/g, ' ')}${lastJsonMessage.details ? ` - ${lastJsonMessage.details}` : ''}`;
          setCurrentStatus(statusText); // Overwrite status

          // Clear status and processing flag if workflow ends
          if (step === 'opt_workflow_end' || step === 'insight_workflow_end') {
              setCurrentStatus(null); 
              setIsProcessing(false); // <<< SET PROCESSING TO FALSE HERE
              break; 
          }

          // Check for step completions to create milestones
          if (status === 'completed') {
              let milestoneContent = `✅ ${step?.replace(/_/g, ' ')} Finished`;
              let milestoneQueries: { objective: string; query: string }[] | undefined = undefined;

              if ((step === 'generate_opt_queries' || step === 'generate_cypher') && lastJsonMessage.generated_queries) {
                   const queryCount = lastJsonMessage.generated_queries.length;
                   const queryNoun = queryCount === 1 ? 'query' : 'queries';
                   milestoneContent = `✅ Query Generation Finished (${queryCount} ${queryNoun})`;
                   milestoneQueries = lastJsonMessage.generated_queries; 
              } else if (step === 'execute_opt_queries' || step === 'execute_cypher') {
                  // Also check for insight workflow execution step
                  milestoneContent = `✅ Query Execution Finished`;
              } // Add other potential milestone steps here

              // Add the milestone message only if it's a recognized step
              if (step === 'generate_opt_queries' || step === 'generate_cypher' || step === 'execute_opt_queries' || step === 'execute_cypher') {
                  setMessages(prev => [...prev, { 
                      id: generateId(), 
                      role: 'milestone', 
                      content: milestoneContent, 
                      step: step, 
                      generatedQueries: milestoneQueries 
                  }]);
              }
          }
          
          break; // Break after handling normal status/milestones

        case 'reasoning_summary':
          // Add reasoning to the *last* milestone message associated with this step
          if (step && lastJsonMessage.reasoning) {
            const reasoningText = `**Reasoning:**\n${lastJsonMessage.reasoning}`; 
            updateLastMilestone(step, { reasoning: reasoningText });
          }
          break;

        case 'final_insight':
          // Handle final insight message
          const insightContent = lastJsonMessage.insight || 'No final insight received.';
          const insightReasoning = lastJsonMessage.reasoning;
          const insightSuggestions = lastJsonMessage.suggestions || []; // Keep getting suggestions if sent here
          
          setMessages(prev => [...prev, {
            id: generateId(),
            role: 'assistant',
            content: insightContent,
            reasoning: insightReasoning ? `**Final Reasoning:**\n${insightReasoning}` : undefined,
            step: step
          }]);
          
          // Update graph suggestions state if provided with insight message
          // Note: The primary source should be the 'graph_suggestions' message type now
          // setGraphSuggestions(insightSuggestions); // Comment out or remove - let graph_suggestions message handle it

          // --- REMOVE WORKAROUND --- 
          /*
          if (insightSuggestions.length > 0) {
              const firstSuggestionObjective = insightSuggestions[0].objective;
              if (firstSuggestionObjective) {
                  setQueryResults(prev => [
                      ...prev,
                      {
                          objective: firstSuggestionObjective,
                          query: "(Query not directly available from insight message)", 
                          dataframe: undefined, 
                          error: undefined
                      }
                  ]);
              }
          }
          */
          // --- END REMOVAL --- 

          setCurrentStatus(null); 
          setIsProcessing(false);
          break;

        case 'final_recommendation':
           // Handle final recommendations message
           // Expects report_sections now
           const reportSections = lastJsonMessage.report_sections;
           const reportReasoning = lastJsonMessage.reasoning;
           
           // Add final assistant message
           setMessages(prev => [...prev, {
               id: generateId(),
               role: 'assistant',
               // Provide fallback content if sections are missing
               content: reportSections ? `Optimization report generated with ${reportSections.length} sections.` : 'Optimization report received.', 
               reportSections: reportSections, // Store the structured data
               reasoning: reportReasoning ? `**Final Reasoning:**\n${reportReasoning}` : undefined,
               step: step // Optional: associate with final step
           }]);
           // Optimization workflow might not send graph suggestions with the final message
           // Clear old suggestions or handle based on backend logic
           // setGraphSuggestions([]); // Optional: Clear suggestions here if they only come from insight
           setCurrentStatus(null); 
           setIsProcessing(false); 
           break;

        case 'graph_suggestions': // This is the primary handler now
          console.log("Received graph suggestions list:", lastJsonMessage.suggestions);
          setGraphSuggestions(lastJsonMessage.suggestions || []); // Update state with the list
          break;

        case 'query_result': 
           if (lastJsonMessage.objective && lastJsonMessage.query) {
               const newResult: QueryResult = {
                   objective: lastJsonMessage.objective,
                   query: lastJsonMessage.query,
                   dataframe: lastJsonMessage.data,
                   error: lastJsonMessage.error,
               };
               setQueryResults(prev => [...prev, newResult]); // Update data pane state
           }
           break;

        case 'error':
          const errorMsg = `**Error (${step || 'Unknown Step'}):** ${lastJsonMessage.message}${lastJsonMessage.details ? `\\n\\\`\\\`\\\`\\n${lastJsonMessage.details}\\\`\\\`\\\`` : ''}`;
          setMessages(prev => [...prev, { id: generateId(), role: 'system', content: errorMsg }]);
          setCurrentStatus(null); 
          setIsProcessing(false); // <<< AND HERE
          break;

        default:
          console.warn('Received unknown WebSocket message type:', type);
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lastJsonMessage]); // Rerun when a new message arrives

  // --- Send message function --- 
  const sendMessage = useCallback((message: string) => {
    if (readyState === ReadyState.OPEN) {
      if (!message.trim()) return; // Don't send empty messages
     
      setMessages(prev => [...prev, { id: generateId(), role: 'user', content: message }]);
      setQueryResults([]); 
      setCurrentStatus('Processing...'); 
      setIsProcessing(true); // <<< SET PROCESSING TO TRUE HERE
      
      sendWebSocketMessage(message); // Backend expects plain text query
    } else {
      console.error('Cannot send message, WebSocket is not open. State:', readyState);
      setMessages(prev => [...prev, { id: generateId(), role: 'system', content: 'Error: Could not connect to the assistant. Please check the backend server.' }]);
      setIsProcessing(false); // Reset if connection fails
    }
  }, [readyState, sendWebSocketMessage]); // Added currentAssistantMessage dependency

  // Connection status string
  const connectionStatus = {
    [ReadyState.CONNECTING]: 'Connecting',
    [ReadyState.OPEN]: 'Connected',
    [ReadyState.CLOSING]: 'Closing',
    [ReadyState.CLOSED]: 'Disconnected',
    [ReadyState.UNINSTANTIATED]: 'Uninstantiated',
  }[readyState];

  return {
    messages,
    queryResults,
    currentStatus, // Expose current status
    isProcessing, // <<< EXPOSE isProcessing
    graphSuggestions, // Expose list of graph suggestions
    sendMessage,
    connectionStatus,
    readyState,
  };
}
