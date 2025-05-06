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

// Define the structure for query results from the backend
export interface QueryResult {
    objective: string;
    query: string;
    dataframe?: Record<string, unknown>[]; // Changed any to unknown
    error?: string;
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
    graph_suggestions?: Record<string, unknown>[]; // Changed any to unknown
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
}

// Define the specific structure for Graph Suggestions
interface GraphSuggestion {
    objective: string; 
    // Add other known properties if available, e.g., type, columns
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
  }, !!WEBSOCKET_URL); // Only connect if the URL is defined

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
            // Optional: Add a system message indicating connection success
            // setMessages(prev => [...prev, { id: generateId(), role: 'system', content: 'Connection established.'}]);
          } else {
            console.error('connection_established message received without user_id');
          }
          break;
          
        case 'status':
          const statusText = `**${step?.replace(/_/g, ' ')}**: ${status?.replace(/_/g, ' ')}${lastJsonMessage.details ? ` - ${lastJsonMessage.details}` : ''}`;
          setCurrentStatus(statusText); // Overwrite status

          // Clear status and processing flag if workflow ends
          // --- EDIT: Use endsWith for more robust checking --- 
          if (step?.endsWith('workflow_end')) {
              setCurrentStatus(null); 
              setIsProcessing(false); // <<< SET PROCESSING TO FALSE HERE
          } else if (status === 'completed') {
              // Handle step completions to create milestones
              let milestoneContent = `✅ ${step?.replace(/_/g, ' ')} Finished`;
              let milestoneQueries: { objective: string; query: string }[] | undefined = undefined;

              // Updated milestone detection
              if (step?.includes('generate') && step?.includes('queries') && lastJsonMessage.generated_queries) {
                   const queryCount = lastJsonMessage.generated_queries.length;
                   const queryNoun = queryCount === 1 ? 'query' : 'queries';
                   milestoneContent = `✅ Query Generation Finished (${queryCount} ${queryNoun})`;
                   milestoneQueries = lastJsonMessage.generated_queries; 
              } else if (step?.includes('execute') && step?.includes('queries')) {
                  milestoneContent = `✅ Query Execution Finished`;
              } else if (step?.includes('classification')) {
                  milestoneContent = `✅ Classification Finished`; // Example: Add milestone for classification
              } // Add other potential milestone steps here

              // Add the milestone message only if it's a recognized step completion
              if (step?.includes('generate') || step?.includes('execute') || step?.includes('classification')) { // Simplified check
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
          // Add reasoning to the *last* milestone message associated with this step
          if (step && lastJsonMessage.reasoning) {
            const reasoningText = `**Reasoning:**\n${lastJsonMessage.reasoning}`; 
            // Find the most recent milestone for this step and update it
            // This assumes milestones are added *before* reasoning arrives
            setMessages(prev => {
                const lastMilestoneIndex = prev.findLastIndex(m => m.role === 'milestone' && m.step === step);
                if (lastMilestoneIndex !== -1) {
                    const updatedMessages = [...prev];
                    updatedMessages[lastMilestoneIndex] = { ...updatedMessages[lastMilestoneIndex], reasoning: reasoningText };
                    return updatedMessages;
                }
                return prev; // No matching milestone found
            });
          }
          break;

        case 'final_insight':
          // Handle final insight message
          const insightContent = lastJsonMessage.insight || 'No final insight received.';
          const insightReasoning = lastJsonMessage.reasoning;
          const insightSuggestions = lastJsonMessage.graph_suggestions || []; // Keep getting suggestions if sent here
          
          setMessages(prev => [...prev, {
            id: generateId(),
            role: 'assistant',
            content: insightContent,
            reasoning: insightReasoning ? `**Final Reasoning:**\n${insightReasoning}` : undefined,
            step: step
          }]);
          
          if (insightSuggestions.length > 0) {
            console.log("Received graph suggestions within final_insight:", insightSuggestions);
            setGraphSuggestions(insightSuggestions as GraphSuggestion[]); 
          } else {
            console.log("No graph suggestions found in final_insight message.");
          }

          setCurrentStatus(null); 
          setIsProcessing(false);
          break;

        case 'final_recommendation':
           // Handle final recommendations message
           const reportSections = lastJsonMessage.report_sections;
           const reportReasoning = lastJsonMessage.reasoning;
           const suggestions = lastJsonMessage.graph_suggestions;
           if (suggestions && Array.isArray(suggestions)) {
              console.log("Received graph suggestions within final_recommendation:", suggestions);
              setGraphSuggestions(suggestions as GraphSuggestion[]); 
           } else {
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
           setCurrentStatus(null); 
           setIsProcessing(false); 
           break;

        case 'graph_suggestions': // This is the primary handler now
          console.log("Received graph suggestions list:", lastJsonMessage.graph_suggestions);
          setGraphSuggestions((lastJsonMessage.graph_suggestions || []) as GraphSuggestion[]); // Update state with the list
          break;

        case 'query_result':
          // This handles the raw data for the right-hand pane (tables)
          const queryResult: QueryResult = {
            objective: lastJsonMessage.objective || 'Unknown Objective',
            query: lastJsonMessage.query || 'Unknown Query',
            dataframe: lastJsonMessage.data || [], 
            error: lastJsonMessage.error,
          };
          console.log("[useChat] Processing query_result:", queryResult); // Log before state update
          setQueryResults(prev => {
              console.log("[useChat] queryResults state BEFORE update:", prev); // Log previous state
              // Avoid duplicates based on objective AND query string
              const exists = prev.some(qr => qr.objective === queryResult.objective && qr.query === queryResult.query);
              if (!exists) {
                  const newState = [...prev, queryResult];
                  console.log("[useChat] queryResults state AFTER update (adding new):", newState);
                  return newState;
              }
              console.log("[useChat] queryResults state AFTER update (duplicate skipped):", prev);
              return prev; // If duplicate, return previous state
          });
          break;

        case 'routing_decision': // Handle routing info if needed
            console.log('Routing decision:', lastJsonMessage);
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
  }, [lastJsonMessage]); // Rerun when a new message arrives

  // --- Send message function (Modified to use API proxy and handle direct responses) --- 
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
   
    // Preserve existing context parsing logic
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

    // Add UI messages immediately
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
    
    // Reset UI states for processing - Start with Thinking...
    setQueryResults([]); 
    setGraphSuggestions([]); 
    setCurrentStatus('Thinking...'); 
    setIsProcessing(true); 
    
    // Send the ORIGINAL message (with potential context markers) 
    // to the frontend API proxy route
    try {
      const response = await fetch('/api/frontend/chat', { // Use the relative path to our proxy
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: message, // Send the original message string
          userId: userId  // Send the stored user ID
        }),
      });

      if (!response.ok) {
        // Handle HTTP errors from the proxy/agent
        const errorData = await response.json().catch(() => ({ error: `HTTP error ${response.status}` })); // Graceful JSON parsing
        console.error('Error response from /api/frontend/chat:', errorData);
        const errorContent = `Error sending message to agent: ${errorData.detail || errorData.error || response.statusText}`;
        setMessages(prev => [...prev, { id: generateId(), role: 'system', content: errorContent }]);
        setCurrentStatus(null);
        setIsProcessing(false);
      } else {
        // Handle successful response from the proxy/agent
        const agentAckData = await response.json(); 
        console.log("Agent acknowledgement:", agentAckData);
        
        // Add the agent's text response immediately
        if (agentAckData.response) {
            setMessages(prev => [...prev, { 
                id: generateId(), 
                role: 'assistant', 
                content: agentAckData.response 
            }]);
        }

        // Decide whether to keep processing based on whether a tool was called
        if (agentAckData.tool_called) {
            // Tool was called, keep processing status, expect WebSocket updates
            setCurrentStatus('Agent processing workflow...'); 
        } else {
            // No tool called, agent answered directly. Clear status.
            setCurrentStatus(null);
            setIsProcessing(false);
        }
      }
    } catch (error) {
      // Handle network errors or other issues calling the proxy
      console.error('Failed to fetch /api/frontend/chat:', error);
      let errorText = 'Network error connecting to the agent.';
      if (error instanceof Error) {
        errorText = error.message;
      }
      setMessages(prev => [...prev, { id: generateId(), role: 'system', content: `Error: ${errorText}` }]);
      setCurrentStatus(null);
      setIsProcessing(false);
    }

  }, [readyState, userId]); // Add userId as a dependency

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
    currentStatus,
    isProcessing,
    graphSuggestions,
    sendMessage,
    connectionStatus,
    userId, // Expose userId if needed elsewhere
    readyState,
  };
}
