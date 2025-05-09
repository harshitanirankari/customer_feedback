import React from "react";
import { useNavigate } from "react-router-dom";

export default function Home() {
  const navigate = useNavigate();
  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gradient-to-br from-blue-100 to-indigo-200">
      <div className="bg-white rounded-xl shadow-lg p-10 flex flex-col items-center">
        <img src="https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f916.png" alt="Bot" className="w-28 mb-6 animate-bounce" />
        <h1 className="text-3xl font-bold mb-2 text-indigo-700">Welcome to the Customer Feedback Chatbot!</h1>
        <p className="mb-6 text-gray-600 text-center max-w-md">
          Get instant, AI-powered insights from customer feedback. Click below to start chatting with our smart bot!
        </p>
        <button
          className="px-8 py-3 bg-indigo-600 text-white rounded-full font-semibold shadow hover:bg-indigo-700 transition"
          onClick={() => navigate('/chat')}
        >
          Get Started
        </button>
      </div>
    </div>
  );
}
