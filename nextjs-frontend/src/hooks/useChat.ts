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
    type: string; // 'status', 'reasoning_summary', 'final_insight', 'final_recommendations', 'error', 'generated_queries', 'query_result', 'classifier_answer', 'classifier_info', 'routing_decision', 'graph_suggestions'
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

// Update URL to the deployed Render backend (using wss for https)
const WEBSOCKET_URL = 'wss://backend-graphdb.onrender.com/api/v1/chat/stream'; 

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
  const [graphSuggestions, setGraphSuggestions] = useState<GraphSuggestion[]>([]); // Use the specific interface

  const {
    sendMessage: sendWebSocketMessage,
    lastJsonMessage,
    readyState,
  } = useWebSocket<WebSocketMessage>(WEBSOCKET_URL, {
    share: false, // Each component instance gets its own connection (if needed, else true)
    shouldReconnect: () => true, // Removed unused _closeEvent parameter
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

        case 'classifier_info':
            if (typeof lastJsonMessage.content === 'string' && lastJsonMessage.content.trim() !== '') {
                 // --- EDIT: Assign to new variable first ---
                 const messageContent = lastJsonMessage.content; 
                 setMessages(prev => [...prev, { 
                     id: generateId(), 
                     role: 'assistant', 
                     content: messageContent // Use the new variable
                 }]);
                 setCurrentStatus("Planning workflow..."); 
            } else {
                console.warn("Received classifier_info with no valid content.");
            }
            break;
            
        case 'classifier_answer':
             if (typeof lastJsonMessage.content === 'string' && lastJsonMessage.content.trim() !== '') {
                 // --- EDIT: Assign to new variable first ---
                 const messageContent = lastJsonMessage.content; 
                 setMessages(prev => [...prev, { 
                     id: generateId(), 
                     role: 'assistant', 
                     content: messageContent // Use the new variable
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
            updateLastMilestone(step, { reasoning: reasoningText });
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
            // Cast received data to GraphSuggestion[] before setting state
            setGraphSuggestions(insightSuggestions as GraphSuggestion[]); 
          } else {
            console.log("No graph suggestions found in final_insight message.");
          }

          setCurrentStatus(null); 
          setIsProcessing(false);
          break;

        case 'final_recommendation':
           // Handle final recommendations message
           // Expects report_sections now
           const reportSections = lastJsonMessage.report_sections;
           const reportReasoning = lastJsonMessage.reasoning;
           // Also check for graph suggestions within the final message
           const suggestions = lastJsonMessage.graph_suggestions;
           if (suggestions && Array.isArray(suggestions)) {
              console.log("Received graph suggestions within final_recommendation:", suggestions);
              // Cast received data to GraphSuggestion[] before setting state
              setGraphSuggestions(suggestions as GraphSuggestion[]); // Update state with the list
           } else {
              // Optional: Clear suggestions if none are provided with the final report
              // setGraphSuggestions([]); 
              console.log("No graph suggestions found in final_recommendation message.");
           }
           
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
          console.log("Received graph suggestions list:", lastJsonMessage.graph_suggestions);
          // Cast received data to GraphSuggestion[] before setting state
          setGraphSuggestions((lastJsonMessage.graph_suggestions || []) as GraphSuggestion[]); // Update state with the list
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

        case 'routing_decision': // Handle routing info if needed
            console.log('Routing decision:', lastJsonMessage);
            // You could potentially display this info or use workflow_type
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

  // --- Send message function --- 
  const sendMessage = useCallback((message: string) => {
    if (readyState === ReadyState.OPEN) {
      if (!message.trim()) return; 
     
      // --- EDIT: Updated parsing logic to add two messages ---
      let contextMessageToAdd: ChatMessage | null = null;
      let userMessageContent = message; 

      // --- EDIT: Define new markers ---
      const displayContextStartMarker = "---DISPLAY_CONTEXT START---";
      const displayContextEndMarker = "---DISPLAY_CONTEXT END---";
      const queryStartMarker = "---QUERY START---";

      // Check if the message contains our structured context/query format
      if (message.includes(displayContextStartMarker) && message.includes(queryStartMarker)) {
        try {
            // Extract the query
            const queryStartIndex = message.indexOf(queryStartMarker) + queryStartMarker.length;
            userMessageContent = message.substring(queryStartIndex).trim(); 

            // Extract the display context string
            const displayContextStartIndex = message.indexOf(displayContextStartMarker) + displayContextStartMarker.length;
            const displayContextEndIndex = message.indexOf(displayContextEndMarker, displayContextStartIndex); 
            
            if (displayContextEndIndex !== -1 && displayContextEndIndex > displayContextStartIndex) {
                const displayContextString = message.substring(displayContextStartIndex, displayContextEndIndex).trim(); 
                if (displayContextString) {
                    // --- EDIT: Create context message using the display string ---
                    contextMessageToAdd = {
                        id: generateId(),
                        role: 'context_info',
                        content: displayContextString // Use the display string for UI
                    };
                }
            } else {
                console.warn("Couldn't find display context markers or context was empty.");
            }

            // We don't need to extract the backend context here, 
            // as the original 'message' containing it is sent to the backend.

        } catch (e) {
            console.error("Error parsing context/query message:", e);
            userMessageContent = message; // Fallback to original message
            contextMessageToAdd = null;
        }
      } 
      // --- End Parsing Edit ---

      // Add messages to state
      setMessages(prev => {
          const newMessages: ChatMessage[] = [];
          if (contextMessageToAdd) {
              newMessages.push(contextMessageToAdd); // Add context message first
          }
          // Add the user query message
          newMessages.push({
              id: generateId(), // Unique ID for user message
              role: 'user',
              content: userMessageContent // Use the parsed query content
          });
          return [...prev, ...newMessages]; // Append new message(s)
      });
      
      // Reset UI states
      setQueryResults([]); 
      setCurrentStatus('Processing...'); 
      setIsProcessing(true); 
      
      // Send the ORIGINAL combined message to backend
      sendWebSocketMessage(message); 

    } else {
      console.error('Cannot send message, WebSocket is not open. State:', ReadyState[readyState]);
      setMessages(prev => [...prev, { id: generateId(), role: 'system', content: 'Error: Could not connect to the assistant. Please check the backend server.' }]);
      setIsProcessing(false); // Reset if connection fails
    }
  }, [readyState, sendWebSocketMessage]); // Dependencies

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
