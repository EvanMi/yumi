import logo from './logo.svg';
import './App.css';
import { useState } from 'react';

function App() {

  const [defaultDatas, setDefault] = useState([]);
  const [yumiDatas, setYumi] = useState([]);

  const setupEventStream = () => {
    const eventSource = new EventSource('http://127.0.0.1:8080/stream-sse-mvc');

    eventSource.onopen = (event) => {
     console.log('Connection opened');
    };

    eventSource.onmessage = (event) => {
      setDefault(pre => {
        return pre.length > 10 ? [event.data] : [...pre, event.data]
      });
    };

    eventSource.onerror = (error) => {
      console.error('Error:', error);
      // 在这里处理连接错误
    };

    eventSource.addEventListener("yumi", event => {
      setYumi(pre => {
        return pre.length > 10 ? [event.data] : [...pre, event.data]
      });
    });
  }

  return (
    <div className="App">
     <div><button onClick={setupEventStream}>点我</button></div>
     <div>默认消息</div>
     <ul>{defaultDatas.map(dd => <li>{dd}</li>)}</ul>
     <div>玉米消息</div>
     <ul>{yumiDatas.map(dd => <li>{dd}</li>)}</ul>
    </div>
  );
}

export default App;
