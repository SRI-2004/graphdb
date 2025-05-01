'use client'; // Required for hooks like useState, useEffect

import { useState, useEffect, useRef } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useChat, ChatMessage } from '@/hooks/useChat'; // Removed QueryResult import from here
import { ReadyState } from 'react-use-websocket'; // Import ReadyState as a value
import ReactMarkdown from 'react-markdown'; // To render markdown content
import remarkGfm from 'remark-gfm'; // Support GitHub Flavored Markdown (tables, etc.)
import rehypeRaw from 'rehype-raw'; // Import rehype-raw
import { Loader2 } from 'lucide-react'; // Loading spinner icon
// Import Shadcn Accordion components
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"

// Placeholder for the component that will display tables/graphs
import DataExplorer from '@/components/DataExplorer';

export default function Home() {
  // Get currentStatus, isProcessing, and graphSuggestions list from useChat now
  const { messages, queryResults, sendMessage, readyState, currentStatus, isProcessing, graphSuggestions } = useChat();
  const [inputValue, setInputValue] = useState('');
  const [pendingContext, setPendingContext] = useState<{ display: string; backend: string } | null>(null);
  const chatHistoryRef = useRef<HTMLDivElement>(null);

  const handleSetPendingContext = (context: { display: string; backend: string }) => {
    setPendingContext(context);
    console.log("Context staged: Display=", context.display);
  };

  const handleSend = () => {
    if (!inputValue.trim() && !pendingContext) return;

    let messageToSend = inputValue;
    if (pendingContext) {
      messageToSend = `---DISPLAY_CONTEXT START---${pendingContext.display}---DISPLAY_CONTEXT END------BACKEND_CONTEXT START---${pendingContext.backend}---BACKEND_CONTEXT END------QUERY START---${inputValue}`;
      console.log("Constructed messageToSend:", messageToSend);
    }
    
    sendMessage(messageToSend);
    setInputValue('');
    setPendingContext(null);
  };

  const handleKeyPress = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault(); // Prevent newline on Enter
      handleSend();
    }
  };

  // Scroll to bottom of chat history when messages change
  useEffect(() => {
    if (chatHistoryRef.current) {
      chatHistoryRef.current.scrollTop = chatHistoryRef.current.scrollHeight;
    }
  }, [messages]);

  // Helper to render message content (handling markdown and optional sections)
  const renderMessageContent = (msg: ChatMessage) => {
    // Handle Milestones with <details>
    if (msg.role === 'milestone') {
        return (
            <details className='space-y-1'>
                <summary className='font-medium text-sm cursor-pointer'>{msg.content}</summary>
                {/* Render Reasoning First */}
                {msg.reasoning && (
                    <div className='mt-2 text-xs p-2 border rounded bg-muted/30'>
                        <div className="prose dark:prose-invert prose-xs max-w-none">
                            <ReactMarkdown 
                              remarkPlugins={[remarkGfm]}
                              rehypePlugins={[rehypeRaw]}
                            >
                                {msg.reasoning}
                            </ReactMarkdown>
                         </div>
                    </div>
                )}
                {/* Render Generated Queries Array */}
                {msg.generatedQueries && msg.generatedQueries.length > 0 && (
                     <div className='mt-2 space-y-2'>
                        <h4 className="text-xs font-semibold text-muted-foreground">Generated Queries:</h4>
                        {msg.generatedQueries.map((q, index) => {
                            console.log(`Rendering Query ${index}:`, q);
                            return (
                                <div key={index} className='text-xs p-2 border rounded bg-muted/30'>
                                    <p className="font-medium mb-1">{q.objective || `Query ${index + 1}`}</p>
                                    <div className="prose dark:prose-invert prose-xs max-w-none">
                                        <ReactMarkdown 
                                          remarkPlugins={[remarkGfm]}
                                          rehypePlugins={[rehypeRaw]}
                                        >
                                            {`\\\`\\\`\\\`cypher\\n${q.query}\\n\\\`\\\`\\\``}
                                        </ReactMarkdown>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                )}
            </details>
        );
    }
    
    // Render regular user/assistant/system messages
    // OR render structured report sections using Accordion
    if (msg.role === 'assistant' && msg.reportSections && msg.reportSections.length > 0) {
      // Render Accordion for structured report
      // Determine default open section (e.g., Recommendations)
      const defaultOpenValue = msg.reportSections.find(s => s.title?.includes('Recommendations'))?.title || msg.reportSections[0]?.title;
      
      return (
         <Accordion type="single" collapsible className="w-full" defaultValue={defaultOpenValue}>
           {msg.reportSections.map((section, index) => (
             <AccordionItem value={section.title || `section-${index}`} key={section.title || index}>
               <AccordionTrigger className="text-base font-semibold hover:no-underline">
                 {section.title || `Section ${index + 1}`}
               </AccordionTrigger>
               <AccordionContent>
                 <div className="prose dark:prose-invert prose-sm max-w-none break-words">
                   <ReactMarkdown 
                     remarkPlugins={[remarkGfm]}
                     rehypePlugins={[rehypeRaw]}
                   >
                     {section.content}
                   </ReactMarkdown>
                 </div>
                 {/* Optionally render reasoning if it exists and is associated per section (not current setup) */}
               </AccordionContent>
             </AccordionItem>
           ))}
         </Accordion>
      );
    }
    
    // Handle Context Info messages
    if (msg.role === 'context_info') {
        return (
            <div className="prose dark:prose-invert prose-sm max-w-none break-words">
                <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeRaw]}>
                    {msg.content} 
                </ReactMarkdown>
            </div>
        );
    }
    
    // Handle User Messages with potential context
    if (msg.role === 'user') {
        return (
            <div className="prose dark:prose-invert prose-sm max-w-none break-words">
                <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeRaw]}>
                    {msg.content} 
                </ReactMarkdown>
            </div>
        );
    }
    
    // Fallback for Assistant (non-report), System messages
    return (
      <div className="prose dark:prose-invert prose-sm max-w-none break-words"> 
        <ReactMarkdown 
          remarkPlugins={[remarkGfm]}
          rehypePlugins={[rehypeRaw]} 
        >
          {msg.content} 
        </ReactMarkdown>
      </div>
    );
  };

  return (
    <main className="flex h-screen flex-col p-4 gap-4 bg-background text-foreground">
    
       
       <div className="flex flex-1 flex-row items-stretch gap-4 overflow-hidden"> 
          {/* Left Pane: Chat */}
          <div className="flex-1 flex flex-col min-w-0"> 
            <Card className="flex-1 flex flex-col overflow-hidden"> 
              <CardHeader className="flex-shrink-0">
                <CardTitle>Chat</CardTitle>
              </CardHeader>
              <CardContent className="flex-1 flex flex-col justify-between overflow-hidden p-4">
                {/* Chat History Area */}
                <div ref={chatHistoryRef} className="flex-1 overflow-y-auto mb-4 space-y-4 pr-2">
                  {messages.map((msg) => (
                    <div key={msg.id} className={`flex flex-col ${ (msg.role === 'user' || msg.role === 'context_info') ? 'items-end' : 'items-start' }`}> 
                        <div 
                          className={`rounded-lg shadow-sm ${ 
                            msg.role === 'user' 
                              ? 'max-w-[85%] bg-primary text-primary-foreground p-3'
                              : msg.role === 'assistant' 
                              ? (msg.reportSections && msg.reportSections.length > 0
                                  ? 'w-full bg-muted text-muted-foreground p-3'
                                  : 'max-w-[85%] bg-muted text-muted-foreground p-3 assistant-bubble'
                                )
                              : msg.role === 'milestone' 
                              ? 'w-full bg-blue-100 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300 border border-blue-300 dark:border-blue-700 p-3'
                              : msg.role === 'context_info'
                              ? 'max-w-[85%] bg-blue-600 dark:bg-blue-700 text-blue-50 mb-1 px-2 py-1 text-xs'
                              : 'max-w-[85%] bg-destructive/10 text-destructive border border-destructive/30 p-3'
                          }`}
                        >
                          {renderMessageContent(msg)}
                      </div>
                      {/* --- EDIT: Exclude context_info from role label --- */}
                      {(msg.role !== 'user' && msg.role !== 'context_info') && (
                         <p className="text-xs text-muted-foreground mt-1 capitalize">
                             {msg.role.replace('_', ' ')}
                         </p>
                      )}
                    </div>
                  ))}
                </div>
                 {/* Live Status Indicator Area */} 
                 {currentStatus && (
                    <div className="flex-shrink-0 text-sm text-muted-foreground mb-2 flex items-center gap-2 p-2 border rounded bg-muted/50">
                        <Loader2 className="h-4 w-4 animate-spin" /> 
                         {/* Apply prose to wrapper div */}
                        <div className="prose dark:prose-invert prose-sm max-w-none">
                           <ReactMarkdown 
                             remarkPlugins={[remarkGfm]}
                             rehypePlugins={[rehypeRaw]}
                           >
                             {currentStatus}
                           </ReactMarkdown>
                         </div>
                    </div>
                 )} 
                {/* Chat Input Area */}
                <div className="flex gap-2 flex-shrink-0 pt-4 border-t"> 
                  <Input 
                    type="text" 
                    placeholder={readyState === ReadyState.OPEN ? "Ask about insights..." : "Connecting..."}
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onKeyPress={handleKeyPress}
                    disabled={readyState !== ReadyState.OPEN || currentStatus !== null} // Disable input while processing
                    className="flex-1"
                  />
                  <Button 
                    onClick={handleSend} 
                    disabled={readyState !== ReadyState.OPEN || !inputValue.trim() || currentStatus !== null} // Disable send while processing
                  >
                    Send
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Right Pane: Data Explorer */}
          <div className="flex-[1.5] flex flex-col min-w-0"> 
            <Card className="flex-1 flex flex-col overflow-hidden"> 
              <CardHeader className="flex-shrink-0">
                <CardTitle>Data Explorer</CardTitle>
              </CardHeader>
              <CardContent className="flex-1 overflow-y-auto p-4">
                 <DataExplorer 
                    queryResults={queryResults} 
                    graphSuggestions={graphSuggestions} 
                    isProcessing={isProcessing} 
                    onSetPendingContext={handleSetPendingContext}
                 />
              </CardContent>
            </Card>
          </div>
       </div>
    </main>
  );
}
