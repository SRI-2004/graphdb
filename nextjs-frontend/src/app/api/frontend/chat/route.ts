import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  try {
    // 1. Get the message and userId from the incoming frontend request
    const body = await request.json();
    const { message, userId } = body;

    if (!message || !userId) {
      return NextResponse.json(
        { error: 'Missing message or userId in request body' },
        { status: 400 }
      );
    }

    // 2. Get the Agent API URL from environment variables
    const agentApiUrl = process.env.AGENT_API_URL;
    if (!agentApiUrl) {
      console.error('AGENT_API_URL environment variable is not set.');
      return NextResponse.json(
        { error: 'Agent API URL is not configured on the server.' },
        { status: 500 }
      );
    }

    // 3. Forward the request to the Chatbot Agent backend
    console.log(`Forwarding chat request for userId: ${userId} to ${agentApiUrl}`);
    const agentResponse = await fetch(agentApiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      // Ensure the payload matches what the Python agent expects (user_id)
      body: JSON.stringify({ message: message, user_id: userId }), 
    });

    // 4. Handle the response from the agent
    if (!agentResponse.ok) {
      const errorText = await agentResponse.text();
      console.error(`Error from agent API: ${agentResponse.status} ${errorText}`);
      return NextResponse.json(
        { error: `Failed to get response from agent: ${errorText}` },
        { status: agentResponse.status }
      );
    }

    const agentData = await agentResponse.json();
    console.log(`Received response from agent for userId: ${userId}`, agentData);

    // 5. Return the agent's response back to the frontend client
    // The agent returns a simple object like { response: "..." }
    return NextResponse.json(agentData);

  } catch (error) {
    console.error('Error in /api/frontend/chat:', error);
    let errorMessage = 'Internal Server Error';
    if (error instanceof Error) {
      errorMessage = error.message;
    }
    return NextResponse.json(
      { error: 'An unexpected error occurred', details: errorMessage },
      { status: 500 }
    );
  }
} 