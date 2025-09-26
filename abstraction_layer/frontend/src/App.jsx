import { useState } from 'react';
import './App.css';

function App() {
  const [question, setQuestion] = useState('');
  const [loading, setLoading] = useState(false);
  const [response, setResponse] = useState(null);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResponse(null);

    try {
      const res = await fetch('http://localhost:8000/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ question }),
      });

      if (!res.ok) {
        throw new Error('API request failed');
      }

      const data = await res.json();
      setResponse(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const renderResults = () => {
    if (!response || !response.success || response.results.length === 0) {
      return <p className="no-results">No results found.</p>;
    }

    const allKeys = [...new Set(response.results.flatMap(Object.keys))];

    return (
      <div className="results-container">
        <h3 className="results-header">Found {response.results.length} results for: {response.question}</h3>
        <table className="results-table">
          <thead>
            <tr>
              {allKeys.map((key) => (
                <th key={key} className="table-header">
                  {key}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {response.results.map((result, index) => (
              <tr key={index} className="table-row">
                {allKeys.map((key) => (
                  <td key={key} className="table-cell">
                    {result[key] || '-'}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <div className="app-container">
      <div className="sidebar">
        <ul className="sidebar-menu">
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span></li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
          <li><span className="icon"></span> </li>
        </ul>
      </div>
      <header className="app-header">
        <div className="logo">Query</div>
        <div className="user-number">YOUR NUMBER 16105467627</div>
      </header>
      <main className="main-content">
        <div className="search-card">
          <h1 className="main-title">Query</h1>
          <form className="query-form" onSubmit={handleSubmit}>
            <input
              type="text"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="Enter your query (e.g., Find all architects in Mumbai)"
              className="query-input"
            />
            <button type="submit" disabled={loading} className="submit-button">
              {loading ? 'Querying...' : 'Search'}
            </button>
          </form>
          {error && <p className="error-message">Error: {error}</p>}
          {response && renderResults()}
        </div>
      </main>
    </div>
  );
}

export default App;