import React, { useState } from "react";
import axios from "axios";

export default function Chat() {
  const [messages, setMessages] = useState([
    { sender: "bot", text: "Hi! Ask me anything about customer feedback." }
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const sendMessage = async () => {
    if (!input.trim()) return;
    setMessages([...messages, { sender: "user", text: input }]);
    setLoading(true);
    try {
      const res = await axios.post("http://127.0.0.1:8000/chat", {
        question: input
      });
      
      
      setMessages(msgs => [
        ...msgs,
        { sender: "bot", text: res.data.answer }
      ]);
    } catch (err) {
      setMessages(msgs => [
        ...msgs,
        { sender: "bot", text: "Sorry, something went wrong." }
      ]);
    }
    setInput("");
    setLoading(false);
  };

  return (
    <div className="flex flex-col items-center min-h-screen bg-gradient-to-br from-blue-100 to-indigo-200">
      <div className="w-full max-w-2xl mt-10 bg-white rounded-xl shadow-lg flex flex-col h-[70vh]">
        <div className="flex items-center gap-3 p-4 border-b">
          <img src="https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f916.png" alt="Bot" className="w-10" />
          <span className="font-bold text-indigo-700 text-lg">Chatbot</span>
        </div>
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {messages.map((msg, i) => (
            <div key={i} className={`flex ${msg.sender === "user" ? "justify-end" : "justify-start"}`}>
              <div className={`px-4 py-2 rounded-lg max-w-[70%] ${msg.sender === "user" ? "bg-indigo-600 text-white" : "bg-gray-200 text-gray-800"}`}>
                {msg.text}
              </div>
            </div>
          ))}
          {loading && (
            <div className="flex justify-start">
              <div className="px-4 py-2 rounded-lg bg-gray-200 text-gray-800 animate-pulse">
                Bot is typing...
              </div>
            </div>
          )}
        </div>
        <div className="p-4 border-t flex gap-2">
          <input
            className="flex-1 border rounded-full px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-400"
            type="text"
            placeholder="Type your message..."
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === "Enter" && sendMessage()}
            disabled={loading}
          />
          <button
            className="bg-indigo-600 text-white px-6 py-2 rounded-full font-semibold hover:bg-indigo-700 transition"
            onClick={sendMessage}
            disabled={loading}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
