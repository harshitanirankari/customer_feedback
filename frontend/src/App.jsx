import React from "react";
import { Routes, Route } from "react-router-dom";
// You will create these components soon
import Home from "./Home";
import Chat from "./Chat";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/chat" element={<Chat />} />
    </Routes>
  );
}
