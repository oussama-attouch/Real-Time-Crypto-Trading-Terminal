import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import * as tradeStream from './services/tradeStream';
import './styles/global.css';

const container = document.getElementById('root');
if (container === null) {
  // Loud on purpose: this is the difference between "blank page" and "blank page with a
  // 3-second diagnosis".
  throw new Error('#root element is missing from index.html');
}

/**
 * Started before render, outside React. StrictMode mounts components twice in
 * development, so a `useEffect(() => start())` would connect twice; the module-level
 * `started` flag plus wsService's `if (this.client !== null) return;` guard make this
 * safe to call from anywhere, once.
 */
tradeStream.start();

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
