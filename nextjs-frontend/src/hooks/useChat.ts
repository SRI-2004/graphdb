import { useState, useCallback, useEffect } from 'react';
import useWebSocket, { ReadyState } from 'react-use-websocket';

// Define the structure of a chat message
export interface ChatMessage {
  id: string; // Add unique ID for React keys
  role: 'user' | 'assistant' | 'system' | 'milestone' | 'context_info'; // Add milestone role
  content: string; // Main text or summary for milestone OR fallback text
  // Optional fields based on backend stream
  reasoning?: string;
  reportSections?: { title: string; content: string }[]; // Store structured sections
  // Store the array of queries directly
  generatedQueries?: { objective: string; query: string }[]; 
  step?: string; // Associate milestone with a step
}

// Define structure for individual query results (used in DataExplorer)
export interface QueryResult {
  objective: string;
  query: string;
  dataframe: Record<string, unknown>[]; // Array of data rows
  error?: string;
  platform?: string; // Optional: if we want to tag single query_result messages too
}

// Define structure for graph suggestions
export interface GraphSuggestion {
  objective: string; // To link suggestion to a query objective if needed
  // Add other fields that your backend sends for graph suggestions
  // For example:
  type?: string; // e.g., 'bar', 'line'
  x_axis?: string; // column name for x-axis
  y_axis?: string | string[]; // column name(s) for y-axis
  title?: string;
  description?: string;
  [key: string]: unknown; // Allow other properties
}

// Define the structure of messages coming FROM the WebSocket
interface WebSocketMessage {
    type: string; // 'status', 'reasoning_summary', 'final_insight', 'final_recommendations', 'error', 'generated_queries', 'query_result', 'classifier_answer', 'classifier_info', 'routing_decision', 'graph_suggestions', 'connection_established'
    user_id?: string; // <-- Added user_id for connection_established
    // Add fields based on the backend stream types
    step?: string;
    status?: string;
    details?: string;
    reasoning?: string;
    insight?: string;
    report_sections?: { title: string; content: string }[]; // For structured reports
    graph_suggestions?: GraphSuggestion[]; // Use specific interface
    message?: string; // for errors
    generated_queries?: { objective: string; query: string }[]; // From backend status update
    objective?: string;
    query?: string;
    data?: Record<string, unknown>[]; // Changed any to unknown
    error?: string; // Query execution error
    requires_execution?: boolean; // Added for workflows that might not need query execution
    content?: string;
    workflow_type?: string;
    classification_details?: Record<string, unknown>; // Changed any to unknown
    platform?: string; // For individual query_result messages
    // New field for combined results in final_insight
    executed_queries?: { platform: string, objective: string, query: string, data: Record<string, unknown>[] }[];
}

// Get WebSocket URL from environment variable
const WEBSOCKET_URL = process.env.NEXT_PUBLIC_WEBSOCKET_URL;

if (!WEBSOCKET_URL) {
  console.error("Error: NEXT_PUBLIC_WEBSOCKET_URL environment variable is not set!");
  // Optionally provide a default or throw an error depending on requirements
}

export function useChat() {
  // User ID received from WebSocket connection
  const [userId, setUserId] = useState<string | null>(null);
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
  const [graphSuggestions, setGraphSuggestions] = useState<GraphSuggestion[]>([]); // Use the specific interface

  const {
    // sendMessage: sendWebSocketMessage, // We don't use the hook's sendMessage anymore
    lastJsonMessage,
    readyState,
  } = useWebSocket<WebSocketMessage>(WEBSOCKET_URL || '', { // Use URL or empty string if undefined
    share: false, 
    shouldReconnect: () => true, 
    retryOnError: true, // Attempt to reconnect on error
    onOpen: () => { console.log('WebSocket Connected'); setCurrentStatus(null); setIsProcessing(false); /* userId will be set on message */ },
    onClose: () => { console.log('WebSocket Disconnected'); setUserId(null); setCurrentStatus('Connection closed.'); setIsProcessing(false); },
    onError: (event) => { console.error('WebSocket Error:', event); setUserId(null); setCurrentStatus('Connection error.'); setIsProcessing(false); },
  }, !!WEBSOCKET_URL);

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
        case 'connection_established': // Handle initial connection message
          if (lastJsonMessage.user_id) {
            setUserId(lastJsonMessage.user_id);
            console.log('Received user_id:', lastJsonMessage.user_id);
          } else {
            console.error('connection_established message received without user_id');
          }
          break;
          
        case 'status':
          const statusText = `**${step?.replace(/_/g, ' ')}**: ${status?.replace(/_/g, ' ')}${lastJsonMessage.details ? ` - ${lastJsonMessage.details}` : ''}`;
          setCurrentStatus(statusText);

          if (step?.endsWith('workflow_end')) {
              setCurrentStatus(null); 
              setIsProcessing(false);
          } else if (status === 'completed') {
              let milestoneContent = `✅ ${step?.replace(/_/g, ' ')} Finished`;
              let milestoneQueries: { objective: string; query: string }[] | undefined = undefined;

              if (step?.includes('generate') && step?.includes('queries') && lastJsonMessage.generated_queries) {
                   const queryCount = lastJsonMessage.generated_queries.length;
                   const queryNoun = queryCount === 1 ? 'query' : 'queries';
                   milestoneContent = `✅ Query Generation Finished (${queryCount} ${queryNoun})`;
                   milestoneQueries = lastJsonMessage.generated_queries; 
              } else if (step?.includes('execute') && step?.includes('queries')) {
                  milestoneContent = `✅ Query Execution Finished`;
              } else if (step?.includes('classification')) {
                  milestoneContent = `✅ Classification Finished`;
              }

              if (step?.includes('generate') || step?.includes('execute') || step?.includes('classification')) {
                  setMessages(prev => [...prev, { 
                      id: generateId(), 
                      role: 'milestone', 
                      content: milestoneContent, 
                      step: step, 
                      generatedQueries: milestoneQueries 
                  }]);
              }
          }
          break;

        case 'classifier_info':
            if (typeof lastJsonMessage.content === 'string' && lastJsonMessage.content.trim() !== '') {
                 const messageContent = lastJsonMessage.content; 
                 setMessages(prev => [...prev, { 
                     id: generateId(), 
                     role: 'assistant', 
                     content: messageContent 
                 }]);
                 setCurrentStatus("Planning workflow..."); 
            } else {
                console.warn("Received classifier_info with no valid content.");
            }
            break;
            
        case 'classifier_answer':
             if (typeof lastJsonMessage.content === 'string' && lastJsonMessage.content.trim() !== '') {
                 const messageContent = lastJsonMessage.content; 
                 setMessages(prev => [...prev, { 
                     id: generateId(), 
                     role: 'assistant', 
                     content: messageContent 
                 }]);
                 setCurrentStatus(null); 
                 setIsProcessing(false);
            } else {
                 console.warn("Received classifier_answer with no valid content.");
                 setCurrentStatus(null); 
                 setIsProcessing(false);
            }
            break;

        case 'reasoning_summary':
          if (step && lastJsonMessage.reasoning) {
            const reasoningText = `**Reasoning:**\n${lastJsonMessage.reasoning}`; 
            setMessages(prev => {
                const lastMilestoneIndex = prev.findLastIndex(m => m.role === 'milestone' && m.step === step);
                if (lastMilestoneIndex !== -1) {
                    const updatedMessages = [...prev];
                    updatedMessages[lastMilestoneIndex] = { ...updatedMessages[lastMilestoneIndex], reasoning: reasoningText };
                    return updatedMessages;
                }
                return prev;
            });
          }
          break;

        case 'final_insight':
          const insightContent = lastJsonMessage.insight || 'No final insight received.';
          const insightReasoning = lastJsonMessage.reasoning;
          const insightGraphSuggestions = lastJsonMessage.graph_suggestions || [];
          
          setMessages(prev => [...prev, {
            id: generateId(),
            role: 'assistant',
            content: insightContent,
            reasoning: insightReasoning ? `**Final Reasoning:**\n${insightReasoning}` : undefined,
            step: step
          }]);
          
          // Handle executed_queries: Create separate QueryResult for each executed query
          if (lastJsonMessage.executed_queries && Array.isArray(lastJsonMessage.executed_queries)) {
            console.log("Processing final_insight with executed_queries to create separate table results.");
            const newQueryResultsFromExecuted: QueryResult[] = [];
            lastJsonMessage.executed_queries.forEach((executedQuery, index) => {
              if (executedQuery.data) { // Ensure data exists 
                const queryResult: QueryResult = {
                  objective: executedQuery.objective || `Executed Query ${index + 1}`,
                  query: executedQuery.query || "N/A",
                  dataframe: executedQuery.data || [],
                  platform: executedQuery.platform // Add platform tag
                };
                newQueryResultsFromExecuted.push(queryResult);
              } else {
                  console.warn(`Executed query at index ${index} has no data. Skipping.`);
              }
            });
        
            setQueryResults(newQueryResultsFromExecuted); // Replace existing queryResults with the list of individual results
            console.log("Updated queryResults with separate results from executed_queries:", newQueryResultsFromExecuted);
          } else {
            console.log("Processed final_insight without executed_queries (standard single-platform insight).");
            // For single-platform insights, queryResults are populated by individual 'query_result' messages.
            // If queryResults is empty here, it might mean no query_result messages came or were processed.
            // We might want to check if queryResults is empty and log a warning, but don't clear it.
          }

          // Handle graph suggestions based on the final state
          if (insightGraphSuggestions.length > 0) {
            console.log("Received graph suggestions within final_insight:", insightGraphSuggestions);
            setGraphSuggestions(insightGraphSuggestions as GraphSuggestion[]); 
          } else {
            // Clear suggestions if none provided in the final message
            setGraphSuggestions([]); 
            console.log("No graph suggestions found in final_insight message. Clearing suggestions.");
          }

          setCurrentStatus(null); 
          setIsProcessing(false);
          break;

        case 'final_recommendation':
           const reportSections = lastJsonMessage.report_sections;
           const reportReasoning = lastJsonMessage.reasoning;
           const recommendationGraphSuggestions = lastJsonMessage.graph_suggestions;
           if (recommendationGraphSuggestions && Array.isArray(recommendationGraphSuggestions)) {
              console.log("Received graph suggestions within final_recommendation:", recommendationGraphSuggestions);
              setGraphSuggestions(recommendationGraphSuggestions as GraphSuggestion[]); 
           } else {
              setGraphSuggestions([]); // Clear if no suggestions provided
              console.log("No graph suggestions found in final_recommendation message.");
           }
           
           setMessages(prev => [...prev, {
               id: generateId(),
               role: 'assistant',
               content: reportSections ? `Optimization report generated with ${reportSections.length} sections.` : 'Optimization report received.', 
               reportSections: reportSections, 
               reasoning: reportReasoning ? `**Final Reasoning:**\n${reportReasoning}` : undefined,
               step: step 
           }]);
           
           // Handle executed_queries for general optimization workflow
           if (lastJsonMessage.executed_queries && Array.isArray(lastJsonMessage.executed_queries)) {
             console.log("Processing final_recommendation with executed_queries to create separate table results.");
             const newQueryResultsFromExecuted: QueryResult[] = [];
             lastJsonMessage.executed_queries.forEach((executedQuery, index) => {
               if (executedQuery.data) { // Ensure data exists 
                 const queryResult: QueryResult = {
                   objective: executedQuery.objective || `Executed Query ${index + 1}`,
                   query: executedQuery.query || "N/A",
                   dataframe: executedQuery.data || [],
                   platform: executedQuery.platform // Add platform tag
                 };
                 newQueryResultsFromExecuted.push(queryResult);
               } else {
                 console.warn(`Executed query at index ${index} has no data. Skipping.`);
               }
             });
             
             setQueryResults(newQueryResultsFromExecuted); // Replace existing queryResults with the list of individual results
             console.log("Updated queryResults with separate results from executed_queries:", newQueryResultsFromExecuted);
           } else {
             // For single-platform optimization, typically queryResults are not the primary display
             setQueryResults([]); 
             console.log("Processing final_recommendation without executed_queries (standard single-platform optimization).");
           }
           
           setCurrentStatus(null); 
           setIsProcessing(false); 
           break;

        case 'query_result':
          // This handles the raw data for the right-hand pane (tables)
          // For general insight, this will show intermediate Google/Facebook results before final_insight combines them.
          const queryResultData: QueryResult = {
            objective: lastJsonMessage.objective || 'Unknown Objective',
            query: lastJsonMessage.query || 'Unknown Query',
            dataframe: lastJsonMessage.data || [], 
            error: lastJsonMessage.error,
            platform: lastJsonMessage.platform, // Capture platform if sent
          };
          console.log("[useChat] Processing query_result:", queryResultData);
          setQueryResults(prev => {
              const exists = prev.some(qr => qr.objective === queryResultData.objective && qr.query === queryResultData.query && qr.platform === queryResultData.platform);
              if (!exists) {
                  const newState = [...prev, queryResultData];
                  return newState;
              }
              return prev;
          });
          break;

        case 'routing_decision':
            console.log('Routing decision:', lastJsonMessage);
            break;

        case 'error':
          const errorMsg = `**Error (${step || 'Unknown Step'}):** ${lastJsonMessage.message}${lastJsonMessage.details ? `\\n\\\`\\\`\\\`\\n${lastJsonMessage.details}\\\`\\\`\\\`` : ''}`;
          setMessages(prev => [...prev, { id: generateId(), role: 'system', content: errorMsg }]);
          setCurrentStatus(null); 
          setIsProcessing(false);
          break;

        default:
          console.warn('Received unknown WebSocket message type:', type);
      }
    }
  }, [lastJsonMessage]);

  const sendMessage = useCallback(async (message: string) => {
    if (readyState !== ReadyState.OPEN) {
      console.error('Cannot send message, WebSocket is not open.');
      setMessages(prev => [...prev, { id: generateId(), role: 'system', content: 'Error: Cannot connect to assistant. Backend connection is closed.' }]);
      setIsProcessing(false); 
      return;
    }

    if (!userId) {
      console.error('Cannot send message, user ID not yet received from WebSocket.');
      setMessages(prev => [...prev, { id: generateId(), role: 'system', content: 'Error: Connection established, but user ID not received yet. Please wait a moment and try again.' }]);
      setIsProcessing(false);
      return;
    }
    
    if (!message.trim()) return; 
   
    let contextMessageToAdd: ChatMessage | null = null;
    let userMessageContent = message; 
    const displayContextStartMarker = "---DISPLAY_CONTEXT START---";
    const displayContextEndMarker = "---DISPLAY_CONTEXT END---";
    const queryStartMarker = "---QUERY START---";

    if (message.includes(displayContextStartMarker) && message.includes(queryStartMarker)) {
      try {
          const queryStartIndex = message.indexOf(queryStartMarker) + queryStartMarker.length;
          userMessageContent = message.substring(queryStartIndex).trim(); 
          const displayContextStartIndex = message.indexOf(displayContextStartMarker) + displayContextStartMarker.length;
          const displayContextEndIndex = message.indexOf(displayContextEndMarker, displayContextStartIndex); 
          
          if (displayContextEndIndex !== -1 && displayContextEndIndex > displayContextStartIndex) {
              const displayContextString = message.substring(displayContextStartIndex, displayContextEndIndex).trim(); 
              if (displayContextString) {
                  contextMessageToAdd = {
                      id: generateId(),
                      role: 'context_info',
                      content: displayContextString
                  };
              }
          } else {
              console.warn("Couldn't find display context markers or context was empty.");
          }
      } catch (e) {
          console.error("Error parsing context/query message:", e);
          userMessageContent = message; 
          contextMessageToAdd = null;
      }
    } 

    setMessages(prev => {
        const newMessages: ChatMessage[] = [];
        if (contextMessageToAdd) {
            newMessages.push(contextMessageToAdd); 
        }
        newMessages.push({
            id: generateId(), 
            role: 'user',
            content: userMessageContent 
        });
        return [...prev, ...newMessages];
    });
    
    setQueryResults([]); 
    setGraphSuggestions([]); 
    setCurrentStatus('Thinking...'); 
    setIsProcessing(true);
    
    try {
      const response = await fetch('/api/frontend/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: message, 
          userId: userId
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: `HTTP error ${response.status}` }));
        console.error('Error response from /api/frontend/chat:', errorData);
        const errorContent = `Error sending message to agent: ${errorData.detail || errorData.error || response.statusText}`;
        setMessages(prev => [...prev, { id: generateId(), role: 'system', content: errorContent }]);
        setCurrentStatus(null);
        setIsProcessing(false);
      } else {
        const agentAckData = await response.json(); 
        console.log("Agent acknowledgement:", agentAckData);
        
        if (agentAckData.response) {
            setMessages(prev => [...prev, { 
                id: generateId(), 
                role: 'assistant', 
                content: agentAckData.response 
            }]);
        }

        if (agentAckData.tool_called) {
            setCurrentStatus('Agent processing workflow...'); 
        } else {
            setCurrentStatus(null);
            setIsProcessing(false);
        }
      }
    } catch (error: any) { 
      console.error('Failed to fetch /api/frontend/chat:', error);
      let errorText = 'Network error connecting to the agent.';
      if (error instanceof Error) {
        errorText = error.message;
      }
      setMessages(prev => [...prev, { id: generateId(), role: 'system', content: `Error: ${errorText}` }]);
      setCurrentStatus(null);
      setIsProcessing(false);
    }

  }, [readyState, userId]);

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
    currentStatus,
    isProcessing,
    graphSuggestions,
    sendMessage,
    connectionStatus,
    userId,
    readyState,
  };
}
