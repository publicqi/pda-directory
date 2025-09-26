import React, { useEffect, useState } from 'react';
import { Route, Routes } from 'react-router-dom';

import { DarkModeContext } from './contexts/DarkModeContext';
import SignaturesPage from './pages/Signatures';
import ImportPage from './pages/Import';

const App = () => {
  const [darkMode, setDarkMode] = useState(() => {
    // Check localStorage first, then system preference
    const saved = localStorage.getItem('darkMode');
    if (saved !== null) {
      return JSON.parse(saved);
    }
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  });

  useEffect(() => {
    // Update localStorage when darkMode changes
    localStorage.setItem('darkMode', JSON.stringify(darkMode));

    // Update the data-theme attribute on document root
    document.documentElement.setAttribute('data-theme', darkMode ? 'dark' : 'light');
  }, [darkMode]);

  return (
    <DarkModeContext.Provider value={[darkMode, setDarkMode]}>
      <Routes>
        <Route path="/" element={<SignaturesPage />} />
        <Route path="/import" element={<ImportPage />} />
        <Route path="*" element={<SignaturesPage />} />
      </Routes>
    </DarkModeContext.Provider>
  );
};

export default App;
